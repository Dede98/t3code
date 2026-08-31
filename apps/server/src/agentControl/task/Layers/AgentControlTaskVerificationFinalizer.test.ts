// @effect-diagnostics nodeBuiltinImport:off - captures a committed fixture from a real child-process production harness.
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  AgentControlAttemptId,
  AgentControlRunOnceId,
  AgentControlControlledThreadReservationId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskFinalizedAfterVerificationPayload,
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type AgentControlStageRunLeaseEvent,
  type AgentControlTaskEvent,
  type AgentControlTaskEventDraft,
} from "@t3tools/contracts";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { NodeSqliteTransactionHooks } from "../../../persistence/Services/NodeSqliteTransactionHooks.ts";
import { AgentControlTaskEngineLayerLive } from "../../runtimeLayer.ts";
import { canonicalJson, sha256Utf8, type JsonValue } from "../../initialPlanning/eventEvidence.ts";
import { fingerprintAgentControlSourceIdentity } from "../../stageRun/identity.ts";
import {
  AgentControlStageRunLeaseEngine,
  type AgentControlStageRunLeaseEngineShape,
} from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  deriveVerificationFinalizationCommandId,
  deriveVerificationFinalizationEvidenceId,
  deriveVerificationFinalizationMarkerId,
  deriveVerificationFinalizationReceiptId,
  deriveVerificationLeaseReleaseEventId,
  deriveVerificationTerminalStageEventId,
  fingerprintVerificationTurn,
} from "../../verificationTurn/identity.ts";
import {
  AgentControlTaskEngine,
  type AgentControlTaskEngineShape,
} from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskProjection } from "../Services/AgentControlTaskProjection.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskVerificationFinalizer } from "../Services/AgentControlTaskVerificationFinalizer.ts";
import {
  AgentControlTaskVerificationFinalizerHooks,
  type AgentControlTaskVerificationFinalizerHooksShape,
} from "../Services/AgentControlTaskVerificationFinalizerHooks.ts";
import { layer as TaskEventStoreLive } from "./AgentControlTaskEventStore.ts";
import { layer as TaskProjectionLive } from "./AgentControlTaskProjection.ts";
import { layer as TaskStateRepositoryLive } from "./AgentControlTaskStateRepository.ts";
import { AgentControlTaskVerificationFinalizerLive } from "./AgentControlTaskVerificationFinalizer.ts";
import { loadRunOnceTerminalAuthority } from "../../runOnce/authority.ts";

type Outcome = "passed" | "failed-verdict" | "invalid-output" | "delivery-failed" | "interrupted";

const createdAt = "2026-08-30T09:00:00.000Z";
const sourceChangedAt = "2026-08-30T09:30:00.000Z";
const finalizedAt = "2026-08-30T10:00:00.000Z";

const unavailable = () => Effect.die(new Error("unexpected test dependency call"));

const defaultHooks: AgentControlTaskVerificationFinalizerHooksShape = {
  beforeTransaction: () => Effect.void,
  afterAuthoritativeRead: () => Effect.void,
  afterTaskProjection: () => Effect.void,
  afterEvidence: () => Effect.void,
  afterReceipt: () => Effect.void,
  beforeMarker: () => Effect.void,
  afterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};

const capturePopulatedProduction061 = (
  controlDirectory: string,
  snapshotFilename: string,
): Promise<void> =>
  new Promise((resolve, reject) => {
    const acknowledgementSocket = NodePath.join(
      "/tmp",
      `t3-task-finalization-061-${process.pid}-${NodeCrypto.randomUUID()}.sock`,
    );
    const preload = NodeURL.pathToFileURL(
      NodePath.join(
        process.cwd(),
        "apps/server/src/agentControl/task/testing/captureProduction061OnCleanup.mjs",
      ),
    ).href;
    const childEnvironment: NodeJS.ProcessEnv = {
      ...process.env,
      PATH: `/opt/homebrew/opt/node@24/bin:${process.env.PATH ?? ""}`,
      TMPDIR: controlDirectory,
      NODE_OPTIONS: [process.env.NODE_OPTIONS, `--import=${preload}`].filter(Boolean).join(" "),
      T3_TASK_FINALIZATION_061_ACK_SOCKET: acknowledgementSocket,
      T3_TASK_FINALIZATION_061_SNAPSHOT: snapshotFilename,
    };
    delete childEnvironment.ELECTRON_RUN_AS_NODE;
    const output: Array<string> = [];
    let captured = false;
    let settled = false;
    const server = NodeNet.createServer((socket) => {
      let request = "";
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        request += chunk;
        if (!request.includes("\n")) return;
        try {
          assert.equal(request.trim(), "snapshot-ready");
          const database = new NodeSqlite.DatabaseSync(snapshotFilename, { readOnly: true });
          try {
            const authority = database
              .prepare(`SELECT
                (SELECT count(*) FROM main.effect_sql_migrations WHERE migration_id = 61)
                  AS migration,
                (SELECT count(*) FROM main.agent_control_verification_finalization_markers)
                  AS markers`)
              .get() as { readonly markers: number; readonly migration: number };
            assert.deepStrictEqual(authority, { markers: 1, migration: 1 });
          } finally {
            database.close();
          }
          captured = true;
          socket.end("ack\n");
        } catch (cause) {
          socket.end("error\n");
          if (!settled) {
            settled = true;
            reject(cause);
          }
        }
      });
    });
    const closeServer = () => {
      server.close();
      if (NodeFS.existsSync(acknowledgementSocket)) NodeFS.unlinkSync(acknowledgementSocket);
    };
    server.once("error", (cause) => {
      if (settled) return;
      settled = true;
      reject(cause);
    });
    server.listen(acknowledgementSocket, () => {
      const child = NodeChildProcess.spawn(
        NodePath.join(process.cwd(), "node_modules/.bin/vp"),
        [
          "test",
          "run",
          "apps/server/src/agentControl/initialPlanning/Layers/AgentControlInitialPlanningFinalizer.test.ts",
          "-t",
          "a populated production 060 database migrates to 061 and finalizes 'delivery-failed' exactly once",
        ],
        {
          cwd: process.cwd(),
          env: childEnvironment,
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      child.stdout.on("data", (chunk) => output.push(String(chunk)));
      child.stderr.on("data", (chunk) => output.push(String(chunk)));
      child.once("error", (cause) => {
        closeServer();
        if (settled) return;
        settled = true;
        reject(cause);
      });
      child.once("close", (code) => {
        closeServer();
        if (settled) return;
        settled = true;
        if (code === 0 && captured) resolve();
        else {
          reject(
            new Error(
              `production 061 fixture ${
                captured ? "child failed" : "was not captured"
              }\n${output.join("")}`,
            ),
          );
        }
      });
    });
  });

const makeSource = (suffix: string) => ({
  projectId: ProjectId.make(`task-finalizer-project-${suffix}`),
  repositoryNodeId: `repository-${suffix}`,
  issueNodeId: `issue-${suffix}`,
  issueNumber: 1,
  issueUrl: `https://example.invalid/issues/${suffix}`,
});

const makeCreatedDraft = (
  suffix: string,
): Extract<AgentControlTaskEventDraft, { readonly type: "agentControl.task.created" }> => {
  const taskId = AgentControlTaskId.make(`task-${suffix}`);
  const source = makeSource(suffix);
  const commandId = CommandId.make(`create-task-${suffix}`);
  return {
    eventId: EventId.make(`task-source-event-${suffix}`),
    type: "agentControl.task.created",
    aggregateKind: "task",
    aggregateId: taskId,
    occurredAt: createdAt,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      taskId,
      source,
      status: "candidate",
      sourceGate: "eligible",
      stage: "intake",
      sourceUpdatedAt: createdAt,
      githubIntakeSequence: 1,
      sourceSnapshot: {
        repositoryNodeId: source.repositoryNodeId,
        issueNodeId: source.issueNodeId,
        number: source.issueNumber,
        url: source.issueUrl,
        state: "open",
        title: `Task ${suffix}`,
        body: null,
        contentTrust: "untrusted-external",
        updatedAt: createdAt,
        timelineComplete: true,
        ready: true,
        paused: false,
        eligible: true,
        eligibilityReason: "eligible",
      },
      createdAt,
    },
  };
};

const withInsertGuardsDisabled = (
  database: NodeSqlite.DatabaseSync,
  table: string,
  body: () => void,
) => {
  const triggers = database
    .prepare(
      `SELECT name, sql FROM main.sqlite_schema
       WHERE type = 'trigger' AND tbl_name = ? AND sql LIKE '%BEFORE INSERT%'`,
    )
    .all(table) as unknown as ReadonlyArray<{ readonly name: string; readonly sql: string }>;
  for (const trigger of triggers) {
    database.exec(`DROP TRIGGER main."${trigger.name.replaceAll('"', '""')}"`);
  }
  try {
    body();
  } finally {
    for (const trigger of triggers) database.exec(trigger.sql);
  }
};

const withUpdateGuardsDisabled = (
  database: NodeSqlite.DatabaseSync,
  table: string,
  body: () => void,
) => {
  const triggers = database
    .prepare(
      `SELECT name, sql FROM main.sqlite_schema
       WHERE type = 'trigger' AND tbl_name = ? AND sql LIKE '%BEFORE UPDATE%'`,
    )
    .all(table) as unknown as ReadonlyArray<{ readonly name: string; readonly sql: string }>;
  for (const trigger of triggers) {
    database.exec(`DROP TRIGGER main."${trigger.name.replaceAll('"', '""')}"`);
  }
  try {
    body();
  } finally {
    for (const trigger of triggers) database.exec(trigger.sql);
  }
};

const sourceMapping = (outcome: Outcome, suffix: string = outcome) => {
  switch (outcome) {
    case "passed":
      return {
        deliveryTerminalState: "completed" as const,
        verificationOutcome: "succeeded" as const,
        terminalCause: "verification-passed" as const,
        evaluation: {
          evaluationAuthority: "accepted-evaluation" as const,
          evaluationId: `evaluation-${suffix}`,
          evaluationEvidenceId: `evaluation-evidence-${suffix}`,
          evaluationReceiptId: `evaluation-receipt-${suffix}`,
          evaluationMarkerId: `evaluation-marker-${suffix}`,
          evaluationDisposition: "evaluated" as const,
          verificationVerdict: "passed" as const,
          invalidOutputCode: null,
        },
      };
    case "failed-verdict":
      return {
        deliveryTerminalState: "completed" as const,
        verificationOutcome: "failed" as const,
        terminalCause: "verification-failed" as const,
        evaluation: {
          evaluationAuthority: "accepted-evaluation" as const,
          evaluationId: `evaluation-${suffix}`,
          evaluationEvidenceId: `evaluation-evidence-${suffix}`,
          evaluationReceiptId: `evaluation-receipt-${suffix}`,
          evaluationMarkerId: `evaluation-marker-${suffix}`,
          evaluationDisposition: "evaluated" as const,
          verificationVerdict: "failed" as const,
          invalidOutputCode: null,
        },
      };
    case "invalid-output":
      return {
        deliveryTerminalState: "completed" as const,
        verificationOutcome: "failed" as const,
        terminalCause: "verification-invalid-output" as const,
        evaluation: {
          evaluationAuthority: "accepted-evaluation" as const,
          evaluationId: `evaluation-${suffix}`,
          evaluationEvidenceId: `evaluation-evidence-${suffix}`,
          evaluationReceiptId: `evaluation-receipt-${suffix}`,
          evaluationMarkerId: `evaluation-marker-${suffix}`,
          evaluationDisposition: "invalid-output" as const,
          verificationVerdict: null,
          invalidOutputCode: "schema-violation" as const,
        },
      };
    case "delivery-failed":
      return {
        deliveryTerminalState: "failed" as const,
        verificationOutcome: "failed" as const,
        terminalCause: "provider-delivery-failed" as const,
        evaluation: {
          evaluationAuthority: "not-applicable" as const,
          evaluationId: null,
          evaluationEvidenceId: null,
          evaluationReceiptId: null,
          evaluationMarkerId: null,
          evaluationDisposition: null,
          verificationVerdict: null,
          invalidOutputCode: null,
        },
      };
    case "interrupted":
      return {
        deliveryTerminalState: "interrupted" as const,
        verificationOutcome: "cancelled" as const,
        terminalCause: "provider-delivery-interrupted" as const,
        evaluation: {
          evaluationAuthority: "not-applicable" as const,
          evaluationId: null,
          evaluationEvidenceId: null,
          evaluationReceiptId: null,
          evaluationMarkerId: null,
          evaluationDisposition: null,
          verificationVerdict: null,
          invalidOutputCode: null,
        },
      };
  }
};

const seedCommittedVerificationFinalization = (
  filename: string,
  suffix: string,
  outcome: Outcome,
) => {
  const database = new NodeSqlite.DatabaseSync(filename);
  try {
    database.exec("PRAGMA foreign_keys = OFF");
    const source = makeSource(suffix);
    const taskId = AgentControlTaskId.make(`task-${suffix}`);
    const sourceIdentityFingerprint = fingerprintAgentControlSourceIdentity(source);
    const handoffId = `handoff-${suffix}`;
    const handoffFingerprint = sha256Utf8(`handoff:${suffix}`);
    const finalizationCommandId = deriveVerificationFinalizationCommandId(
      handoffId,
      handoffFingerprint,
    );
    const finalizationEvidenceId = deriveVerificationFinalizationEvidenceId(
      handoffId,
      handoffFingerprint,
    );
    const receiptId = deriveVerificationFinalizationReceiptId(handoffId, handoffFingerprint);
    const markerId = deriveVerificationFinalizationMarkerId(handoffId, handoffFingerprint);
    const stageEventId = deriveVerificationTerminalStageEventId(handoffId, handoffFingerprint);
    const leaseEventId = deriveVerificationLeaseReleaseEventId(handoffId, handoffFingerprint);
    const stageRunId = AgentControlStageRunId.make(`stage-run-${suffix}`);
    const attemptId = AgentControlAttemptId.make(`attempt-${suffix}`);
    const leaseId = AgentControlStageRunLeaseId.make(`lease-${suffix}`);
    const terminalRuntimeEventId = EventId.make(`runtime-terminal-${suffix}`);
    const mapping = sourceMapping(outcome, suffix);
    const common = {
      projectId: source.projectId,
      taskId,
      stageRunId,
      attemptId,
      roleId: "verifier" as const,
      stageKind: "verification" as const,
      stageOrdinal: 3,
      attemptOrdinal: 1,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      admissionEvidenceId: `admission-evidence-${suffix}`,
      admissionReceiptId: `admission-receipt-${suffix}`,
      admissionMarkerId: `admission-marker-${suffix}`,
      materializationEvidenceId: `materialization-evidence-${suffix}`,
      materializationReceiptId: `materialization-receipt-${suffix}`,
      materializationMarkerId: `materialization-marker-${suffix}`,
      startEvidenceId: `start-evidence-${suffix}`,
      startReceiptId: `start-receipt-${suffix}`,
      startMarkerId: `start-marker-${suffix}`,
      handoffId,
      handoffFingerprint,
      providerDeliveryId: `delivery-${suffix}`,
      deliveryRevision: 6,
      claimGeneration: 1,
      attemptCount: 1,
      controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
        `thread-reservation-${suffix}`,
      ),
      threadId: ThreadId.make(`thread-${suffix}`),
      planningThreadId: ThreadId.make(`planning-thread-${suffix}`),
      planId: `plan-${suffix}`,
      proposedPlanDigest: sha256Utf8(`plan:${suffix}`),
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerTurnId: `provider-turn-${suffix}`,
      runtimeMode: "approval-required" as const,
      modelSelectionFingerprint: sha256Utf8(`model:${suffix}`),
      leaseId,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(`holder-${suffix}`),
      fenceToken: 3,
      terminalRuntimeEventId,
      finalizationEvidenceId,
      deliveryTerminalState: mapping.deliveryTerminalState,
      terminalCause: mapping.terminalCause,
      status: mapping.verificationOutcome,
      evaluation: mapping.evaluation,
      finalizedAt,
    } as const;
    const stagePayload = common;
    const leasePayload = {
      leaseId,
      projectId: source.projectId,
      taskId,
      stageRunId,
      attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      holderId: common.leaseHolderId,
      fenceToken: 3,
      admissionEvidenceId: common.admissionEvidenceId,
      admissionReceiptId: common.admissionReceiptId,
      admissionMarkerId: common.admissionMarkerId,
      materializationEvidenceId: common.materializationEvidenceId,
      materializationReceiptId: common.materializationReceiptId,
      materializationMarkerId: common.materializationMarkerId,
      startEvidenceId: common.startEvidenceId,
      startReceiptId: common.startReceiptId,
      startMarkerId: common.startMarkerId,
      handoffId,
      handoffFingerprint,
      controlledThreadReservationId: common.controlledThreadReservationId,
      threadId: common.threadId,
      planningThreadId: common.planningThreadId,
      planId: common.planId,
      proposedPlanDigest: common.proposedPlanDigest,
      providerDeliveryId: common.providerDeliveryId,
      deliveryRevision: 6,
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerTurnId: common.providerTurnId,
      runtimeMode: "approval-required" as const,
      modelSelectionFingerprint: common.modelSelectionFingerprint,
      terminalRuntimeEventId,
      finalizationEvidenceId,
      stageEventId,
      deliveryTerminalState: mapping.deliveryTerminalState,
      terminalCause: mapping.terminalCause,
      stageStatus: mapping.verificationOutcome,
      evaluation: mapping.evaluation,
      releasedAt: finalizedAt,
    };

    withInsertGuardsDisabled(database, "agent_control_events", () => {
      const insert = database.prepare(`INSERT INTO main.agent_control_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_authority,
        payload_json, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      insert.run(
        stageEventId,
        "stage-run",
        stageRunId,
        3,
        mapping.verificationOutcome === "succeeded"
          ? "agentControl.stageRun.verificationSucceeded"
          : mapping.verificationOutcome === "failed"
            ? "agentControl.stageRun.verificationFailed"
            : "agentControl.stageRun.verificationCancelled",
        finalizedAt,
        finalizationCommandId,
        terminalRuntimeEventId,
        finalizationCommandId,
        "system",
        canonicalJson(stagePayload as unknown as JsonValue),
        '{"schemaVersion":1}',
      );
      insert.run(
        leaseEventId,
        "stage-run-lease",
        leaseId,
        8,
        "agentControl.stageRunLease.releasedAfterVerification",
        finalizedAt,
        finalizationCommandId,
        stageEventId,
        finalizationCommandId,
        "system",
        canonicalJson(leasePayload as unknown as JsonValue),
        '{"schemaVersion":1}',
      );
    });
    const stageSequence = Number(
      (
        database
          .prepare("SELECT sequence FROM main.agent_control_events WHERE event_id = ?")
          .get(stageEventId) as { readonly sequence: number }
      ).sequence,
    );
    const leaseSequence = Number(
      (
        database
          .prepare("SELECT sequence FROM main.agent_control_events WHERE event_id = ?")
          .get(leaseEventId) as { readonly sequence: number }
      ).sequence,
    );
    const document = {
      schemaVersion: 1,
      handoffId,
      handoffFingerprint,
      finalizationCommandId,
      finalizationEvidenceId,
      outcome: mapping.verificationOutcome,
      terminalCause: mapping.terminalCause,
      deliveryTerminalState: mapping.deliveryTerminalState,
      terminalRuntimeEventId,
      evaluation: mapping.evaluation,
      stageEventId,
      stageEventSequence: stageSequence,
      stageEventStreamVersion: 3,
      stagePayload,
      leaseEventId,
      leaseEventSequence: leaseSequence,
      leaseEventStreamVersion: 8,
      leasePayload,
      finalizedAt,
    };
    const finalizationJson = canonicalJson(document as unknown as JsonValue);
    const finalizationFingerprint = fingerprintVerificationTurn("finalization-evidence", [
      finalizationJson,
    ]);
    const markerFingerprint = fingerprintVerificationTurn("finalization-marker", [
      handoffId,
      handoffFingerprint,
      String(finalizationCommandId),
      finalizationEvidenceId,
      finalizationFingerprint,
      stageEventId,
      String(stageSequence),
      leaseEventId,
      String(leaseSequence),
      finalizedAt,
    ]);

    withInsertGuardsDisabled(database, "agent_control_verification_handoff_accepted", () => {
      database
        .prepare(
          `INSERT INTO main.agent_control_verification_handoff_accepted VALUES
           (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          handoffId,
          handoffFingerprint,
          common.materializationEvidenceId,
          common.controlledThreadReservationId,
          common.threadId,
          `turn-command-${suffix}`,
          `message-${suffix}`,
          common.providerDeliveryId,
          createdAt,
        );
    });
    withInsertGuardsDisabled(database, "agent_control_verification_finalization_evidence", () => {
      database
        .prepare(`INSERT INTO main.agent_control_verification_finalization_evidence (
          finalization_evidence_id, receipt_id, marker_id, finalization_command_id,
          finalization_fingerprint, finalization_json, handoff_id, handoff_fingerprint,
          project_id, task_id, task_revision, github_intake_sequence,
          source_identity_fingerprint, stage_run_id, attempt_id, lease_id, lease_holder_id,
          fence_token, provider_delivery_id, provider_instance_id, provider_turn_id,
          delivery_revision, delivery_terminal_state, terminal_runtime_event_id, terminal_at,
          start_evidence_id, start_receipt_id, start_marker_id, evaluation_authority,
          evaluation_id, evaluation_evidence_id, evaluation_receipt_id, evaluation_marker_id,
          evaluation_disposition, verification_verdict, invalid_output_code, outcome,
          terminal_cause, stage_event_id, stage_event_sequence, stage_event_stream_version,
          lease_event_id, lease_event_sequence, lease_event_stream_version, finalized_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )`)
        .run(
          finalizationEvidenceId,
          receiptId,
          markerId,
          finalizationCommandId,
          finalizationFingerprint,
          finalizationJson,
          handoffId,
          handoffFingerprint,
          source.projectId,
          taskId,
          1,
          1,
          sourceIdentityFingerprint,
          stageRunId,
          attemptId,
          leaseId,
          common.leaseHolderId,
          3,
          common.providerDeliveryId,
          "codex",
          common.providerTurnId,
          6,
          mapping.deliveryTerminalState,
          terminalRuntimeEventId,
          finalizedAt,
          common.startEvidenceId,
          common.startReceiptId,
          common.startMarkerId,
          mapping.evaluation.evaluationAuthority,
          mapping.evaluation.evaluationId,
          mapping.evaluation.evaluationEvidenceId,
          mapping.evaluation.evaluationReceiptId,
          mapping.evaluation.evaluationMarkerId,
          mapping.evaluation.evaluationDisposition,
          mapping.evaluation.verificationVerdict,
          mapping.evaluation.invalidOutputCode,
          mapping.verificationOutcome,
          mapping.terminalCause,
          stageEventId,
          stageSequence,
          3,
          leaseEventId,
          leaseSequence,
          8,
          finalizedAt,
        );
    });
    withInsertGuardsDisabled(database, "agent_control_verification_finalization_receipts", () => {
      database
        .prepare(`INSERT INTO main.agent_control_verification_finalization_receipts VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )`)
        .run(
          receiptId,
          markerId,
          finalizationEvidenceId,
          finalizationCommandId,
          finalizationFingerprint,
          handoffId,
          mapping.verificationOutcome,
          mapping.terminalCause,
          stageEventId,
          stageSequence,
          leaseEventId,
          leaseSequence,
          "accepted",
          finalizedAt,
        );
    });
    withInsertGuardsDisabled(database, "agent_control_verification_finalization_markers", () => {
      database
        .prepare(`INSERT INTO main.agent_control_verification_finalization_markers VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?
        )`)
        .run(
          markerId,
          markerFingerprint,
          receiptId,
          finalizationEvidenceId,
          finalizationCommandId,
          finalizationFingerprint,
          handoffId,
          finalizedAt,
        );
    });
    const leaseEvent = {
      eventId: leaseEventId,
      type: "agentControl.stageRunLease.releasedAfterVerification",
      aggregateKind: "stage-run-lease",
      aggregateId: leaseId,
      streamVersion: 8,
      sequence: leaseSequence,
      occurredAt: finalizedAt,
      commandId: finalizationCommandId,
      causationEventId: stageEventId,
      correlationId: finalizationCommandId,
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: leasePayload,
    } as unknown as AgentControlStageRunLeaseEvent;
    return {
      handoffId,
      leaseEvent,
      markerId,
      taskId: AgentControlTaskId.make(taskId),
    } as const;
  } finally {
    database.close();
  }
};

const buildRuntime = (
  filename: string,
  scope: Scope.Scope,
  hooks: AgentControlTaskVerificationFinalizerHooksShape = defaultHooks,
  leaseEvents: Stream.Stream<AgentControlStageRunLeaseEvent> = Stream.never,
  beforePublish: (committed: ReadonlyArray<AgentControlTaskEvent>) => Effect.Effect<void> = () =>
    Effect.void,
  taskEngineOverride?: AgentControlTaskEngineShape,
) =>
  Effect.gen(function* () {
    const sqlContext = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(sqlContext, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
    const events = Context.get(
      yield* Layer.buildWithScope(
        Layer.fresh(TaskEventStoreLive).pipe(Layer.provide(sqlLayer)),
        scope,
      ),
      AgentControlTaskEventStore,
    );
    const states = Context.get(
      yield* Layer.buildWithScope(
        Layer.fresh(TaskStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
        scope,
      ),
      AgentControlTaskStateRepository,
    );
    const cursors = Context.get(
      yield* Layer.buildWithScope(
        Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
        scope,
      ),
      AgentControlProjectionStateRepository,
    );
    const projectionDeps = Layer.mergeAll(
      sqlLayer,
      Layer.succeed(AgentControlTaskEventStore, events),
      Layer.succeed(AgentControlTaskStateRepository, states),
      Layer.succeed(AgentControlProjectionStateRepository, cursors),
    );
    const projection = Context.get(
      yield* Layer.buildWithScope(
        Layer.fresh(TaskProjectionLive).pipe(Layer.provide(projectionDeps)),
        scope,
      ),
      AgentControlTaskProjection,
    );
    const publications = yield* Ref.make(0);
    const publishedEvents = yield* Ref.make<ReadonlyArray<string>>([]);
    const publishedEventIds = yield* Ref.make<ReadonlySet<string>>(new Set());
    const taskPubSub = yield* PubSub.unbounded<never>();
    const testTaskEngine = AgentControlTaskEngine.of({
      dispatchController: unavailable,
      dispatchObservedController: unavailable,
      get: unavailable,
      verifySourceSnapshot: unavailable,
      rebuild: unavailable(),
      publishCommitted: (committed) =>
        Effect.forEach(
          committed,
          (event) =>
            Ref.modify(publishedEventIds, (published) => {
              if (published.has(event.eventId)) return [false, published] as const;
              const next = new Set(published);
              next.add(event.eventId);
              return [true, next] as const;
            }).pipe(
              Effect.flatMap((shouldPublish) =>
                shouldPublish
                  ? beforePublish([event]).pipe(
                      Effect.andThen(
                        Effect.all([
                          Ref.update(publications, (count) => count + 1),
                          Ref.update(publishedEvents, (current) => [...current, event.eventId]),
                        ]),
                      ),
                    )
                  : Effect.void,
              ),
            ),
          { discard: true },
        ),
      streamDomainEvents: Stream.fromPubSub(taskPubSub),
      subscribeDomainEvents: Effect.succeed(Stream.fromPubSub(taskPubSub)),
    } satisfies AgentControlTaskEngineShape);
    const taskEngine = taskEngineOverride ?? testTaskEngine;
    const leaseEngine = AgentControlStageRunLeaseEngine.of({
      dispatchController: unavailable,
      dispatchSystem: unavailable,
      toView: unavailable,
      runtimeHolderId: unavailable(),
      rebuild: unavailable(),
      publishCommitted: unavailable,
      streamDomainEvents: leaseEvents,
      subscribeDomainEvents: Effect.succeed(leaseEvents),
    } satisfies AgentControlStageRunLeaseEngineShape);
    const finalizerDeps = Layer.mergeAll(
      projectionDeps,
      Layer.succeed(AgentControlTaskProjection, projection),
      Layer.succeed(AgentControlTaskEngine, taskEngine),
      Layer.succeed(AgentControlStageRunLeaseEngine, leaseEngine),
      Layer.succeed(AgentControlTaskVerificationFinalizerHooks, hooks),
    );
    const finalizer = Context.get(
      yield* Layer.buildWithScope(
        Layer.fresh(AgentControlTaskVerificationFinalizerLive).pipe(Layer.provide(finalizerDeps)),
        scope,
      ),
      AgentControlTaskVerificationFinalizer,
    );
    return {
      events,
      finalizer,
      projection,
      publications,
      publishedEvents,
      sql,
      states,
      taskEngine,
    } as const;
  });

const buildProductionTaskEngine = (filename: string, scope: Scope.Closeable) =>
  Effect.gen(function* () {
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlTaskEngineLayerLive).pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        Layer.provideMerge(NodeServices.layer),
      ),
      scope,
    );
    return Context.get(context, AgentControlTaskEngine);
  });

const seedTask = Effect.fn("seedTaskFinalizerTask")(function* (
  runtime: Effect.Success<ReturnType<typeof buildRuntime>>,
  suffix: string,
) {
  const draft = makeCreatedDraft(suffix);
  const appended = yield* runtime.events.append({
    taskId: draft.aggregateId,
    expectedStreamVersion: 0,
    events: [draft],
  });
  yield* runtime.projection.projectEvent(appended[0]!);
  return appended[0]!;
});

const finalizationCounts = (sql: SqlClient.SqlClient) =>
  sql<{
    readonly evidence: number;
    readonly events: number;
    readonly markers: number;
    readonly publications: number;
    readonly receipts: number;
  }>`
    SELECT
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_evidence) AS evidence,
      (SELECT count(*) FROM main.agent_control_events
        WHERE event_type = 'agentControl.task.finalizedAfterVerification') AS events,
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_markers) AS markers,
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_publications)
        AS publications,
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_receipts) AS receipts
  `;

const taskFinalizationAuthoritySnapshot = (
  sql: SqlClient.SqlClient,
  handoffId: string,
  taskId: AgentControlTaskId,
) =>
  Effect.all({
    evidence: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_task_verification_finalization_evidence
      WHERE handoff_id = ${handoffId}
    `,
    receipts: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_task_verification_finalization_receipts
      WHERE handoff_id = ${handoffId}
    `,
    markers: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_task_verification_finalization_markers
      WHERE handoff_id = ${handoffId}
    `,
    taskEvents: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_events
      WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
      ORDER BY stream_version, sequence
    `,
    projection: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_task_states WHERE task_id = ${taskId}
    `,
    publication: sql<Record<string, unknown>>`
      SELECT * FROM main.agent_control_task_verification_finalization_publications
      WHERE handoff_id = ${handoffId}
    `,
    revisionCounters: sql<Record<string, unknown>>`
      SELECT
        (SELECT count(*) FROM main.agent_control_events
          WHERE aggregate_kind = 'task' AND stream_id = ${taskId}) AS task_event_count,
        (SELECT max(stream_version) FROM main.agent_control_events
          WHERE aggregate_kind = 'task' AND stream_id = ${taskId}) AS task_stream_version,
        (SELECT revision FROM main.agent_control_task_states
          WHERE task_id = ${taskId}) AS projection_revision,
        (SELECT last_event_sequence FROM main.agent_control_task_states
          WHERE task_id = ${taskId}) AS projection_sequence,
        (SELECT revision
          FROM main.agent_control_task_verification_finalization_publications
          WHERE handoff_id = ${handoffId}) AS publication_revision
    `,
    connectionChanges: sql<Record<string, unknown>>`SELECT total_changes() AS changes`,
  });

type TaskFinalizedAfterVerificationEvent = Extract<
  AgentControlTaskEvent,
  { readonly type: "agentControl.task.finalizedAfterVerification" }
>;
const decodeTaskFinalizedAfterVerificationPayloadJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(AgentControlTaskFinalizedAfterVerificationPayload),
);

const taskFinalizationIdentity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${sha256Utf8(
    canonicalJson({ domain: `agent-control-task-${domain}-v1`, parts } as unknown as JsonValue),
  )}`;

const loadPreparedTaskFinalizationEvidence = Effect.fn("loadPreparedTaskFinalizationEvidence")(
  function* (sql: SqlClient.SqlClient, handoffId: string) {
    const rows = yield* sql<{
      readonly commandId: string;
      readonly eventId: string;
      readonly eventSequence: number;
      readonly eventStreamVersion: number;
      readonly payloadJson: string;
    }>`
    SELECT command_id AS "commandId", event_id AS "eventId",
      sequence AS "eventSequence", stream_version AS "eventStreamVersion",
      payload_json AS "payloadJson"
    FROM main.agent_control_events
    WHERE aggregate_kind = 'task'
      AND event_type = 'agentControl.task.finalizedAfterVerification'
      AND json_extract(payload_json, '$.handoffId') = ${handoffId}
  `;
    assert.lengthOf(rows, 1);
    const row = rows[0]!;
    const payload = decodeTaskFinalizedAfterVerificationPayloadJson(
      row.payloadJson,
    ) as TaskFinalizedAfterVerificationEvent["payload"];
    const identityParts = [
      payload.verificationFinalizationMarkerId,
      payload.taskId,
      String(payload.verificationTaskRevision),
    ];
    const receiptId = taskFinalizationIdentity(
      "task-verification-finalization-receipt",
      "verification-finalization-receipt",
      identityParts,
    );
    const markerId = taskFinalizationIdentity(
      "task-verification-finalization-marker",
      "verification-finalization-marker",
      identityParts,
    );
    const finalizationJson = canonicalJson({
      schemaVersion: 1,
      commandId: row.commandId,
      taskFinalizationEvidenceId: payload.taskFinalizationEvidenceId,
      taskFinalizationReceiptId: receiptId,
      taskFinalizationMarkerId: markerId,
      verificationFinalizationEvidenceId: payload.verificationFinalizationEvidenceId,
      verificationFinalizationReceiptId: payload.verificationFinalizationReceiptId,
      verificationFinalizationMarkerId: payload.verificationFinalizationMarkerId,
      verificationFinalizationCommandId: payload.verificationFinalizationCommandId,
      verificationFinalizationFingerprint: payload.verificationFinalizationFingerprint,
      verificationFinalizationMarkerFingerprint: payload.verificationFinalizationMarkerFingerprint,
      taskEventId: row.eventId,
      taskEventStreamVersion: row.eventStreamVersion,
      payload,
      finalizedAt: payload.finalizedAt,
    } as unknown as JsonValue);
    const finalizationFingerprint = sha256Utf8(finalizationJson);
    const markerFingerprint = sha256Utf8(
      canonicalJson({
        domain: "agent-control-task-verification-finalization-marker-v1",
        evidenceId: payload.taskFinalizationEvidenceId,
        receiptId,
        markerId,
        commandId: row.commandId,
        finalizationFingerprint,
        verificationMarkerId: payload.verificationFinalizationMarkerId,
        eventId: row.eventId,
        finalizedAt: payload.finalizedAt,
      } as unknown as JsonValue),
    );
    return {
      ...row,
      finalizationFingerprint,
      finalizationJson,
      markerFingerprint,
      markerId,
      payload,
      receiptId,
    } as const;
  },
);

const insertPreparedTaskFinalizationEvidence = Effect.fn("insertPreparedTaskFinalizationEvidence")(
  function* (
    sql: SqlClient.SqlClient,
    prepared: Effect.Success<ReturnType<typeof loadPreparedTaskFinalizationEvidence>>,
    overrides: {
      readonly previousTaskRevision?: unknown;
      readonly taskSourceEventSequence?: unknown;
    } = {},
  ) {
    const payload = prepared.payload;
    const evaluation = payload.evaluation;
    yield* sql`
    INSERT INTO main.agent_control_task_verification_finalization_evidence (
      task_finalization_evidence_id, receipt_id, marker_id,
      finalization_command_id, finalization_fingerprint, finalization_json,
      verification_evidence_id, verification_receipt_id, verification_marker_id,
      verification_finalization_command_id, verification_finalization_fingerprint,
      verification_finalization_marker_fingerprint, handoff_id, handoff_fingerprint,
      project_id, task_id, verification_task_revision, previous_task_revision,
      github_intake_sequence, source_identity_fingerprint,
      task_source_event_id, task_source_event_sequence, task_source_event_stream_version,
      delivery_terminal_state, verification_outcome, terminal_cause,
      terminal_runtime_event_id, evaluation_authority, evaluation_id,
      evaluation_evidence_id, evaluation_receipt_id, evaluation_marker_id,
      evaluation_disposition, verification_verdict, invalid_output_code,
      terminal_stage_event_id, terminal_stage_event_sequence,
      terminal_stage_run_id, terminal_stage_event_stream_version,
      released_lease_event_id, released_lease_event_sequence,
      released_lease_event_stream_version, released_lease_id,
      task_event_id, task_event_sequence, task_event_stream_version, finalized_at
    ) VALUES (
      ${payload.taskFinalizationEvidenceId}, ${prepared.receiptId}, ${prepared.markerId},
      ${prepared.commandId}, ${prepared.finalizationFingerprint}, ${prepared.finalizationJson},
      ${payload.verificationFinalizationEvidenceId},
      ${payload.verificationFinalizationReceiptId},
      ${payload.verificationFinalizationMarkerId},
      ${payload.verificationFinalizationCommandId},
      ${payload.verificationFinalizationFingerprint},
      ${payload.verificationFinalizationMarkerFingerprint},
      ${payload.handoffId}, ${payload.handoffFingerprint}, ${payload.projectId}, ${payload.taskId},
      ${payload.verificationTaskRevision},
      ${overrides.previousTaskRevision ?? payload.previousTaskRevision},
      ${payload.githubIntakeSequence}, ${payload.sourceIdentityFingerprint},
      ${payload.taskSourceEventId},
      ${overrides.taskSourceEventSequence ?? payload.taskSourceEventSequence},
      ${payload.taskSourceEventStreamVersion}, ${payload.deliveryTerminalState},
      ${payload.verificationOutcome}, ${payload.terminalCause}, ${payload.terminalRuntimeEventId},
      ${evaluation.evaluationAuthority}, ${evaluation.evaluationId},
      ${evaluation.evaluationEvidenceId}, ${evaluation.evaluationReceiptId},
      ${evaluation.evaluationMarkerId}, ${evaluation.evaluationDisposition},
      ${evaluation.verificationVerdict}, ${evaluation.invalidOutputCode},
      ${payload.terminalStageEventId}, ${payload.terminalStageEventSequence},
      ${payload.terminalStageRunId}, ${payload.terminalStageEventStreamVersion},
      ${payload.releasedLeaseEventId}, ${payload.releasedLeaseEventSequence},
      ${payload.releasedLeaseEventStreamVersion}, ${payload.releasedLeaseId},
      ${prepared.eventId}, ${prepared.eventSequence}, ${prepared.eventStreamVersion},
      ${payload.finalizedAt}
    )
  `;
  },
);

const withDatabase = <A, E, R>(
  prefix: string,
  effect: (
    filename: string,
    runtime: Effect.Success<ReturnType<typeof buildRuntime>>,
    scope: Scope.Scope,
  ) => Effect.Effect<A, E, R>,
  hooks: AgentControlTaskVerificationFinalizerHooksShape = defaultHooks,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix });
      const filename = `${directory}/state.sqlite`;
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const runtime = yield* buildRuntime(filename, scope, hooks);
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime.sql),
      );
      return yield* effect(filename, runtime, scope);
    }),
  ).pipe(Effect.provide(NodeServices.layer));

it.live(
  "migrates a populated production 061 authority to 062 and replays after restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-production-061-",
        });
        const filename = `${directory}/production-061.sqlite`;
        yield* Effect.promise(() => capturePopulatedProduction061(directory, filename));

        const firstScope = yield* Scope.make("sequential");
        const first = yield* buildRuntime(filename, firstScope);
        const source = yield* first.sql<{
          readonly finalizationBytes: string;
          readonly finalizationStorage: string;
          readonly handoffId: string;
          readonly legacyGuards: number;
          readonly taskId: string;
        }>`
          SELECT evidence.handoff_id AS "handoffId", evidence.task_id AS "taskId",
            typeof(evidence.finalization_json) AS "finalizationStorage",
            hex(CAST(evidence.finalization_json AS BLOB)) AS "finalizationBytes",
            (SELECT count(*) FROM main.sqlite_schema WHERE type = 'trigger' AND name IN (
              'agent_control_verification_terminal_stage_event_validate',
              'agent_control_verification_lease_release_event_validate',
              'agent_control_verification_finalization_evidence_validate',
              'agent_control_verification_finalization_receipt_validate',
              'agent_control_verification_finalization_marker_validate'
            )) AS "legacyGuards"
          FROM main.agent_control_verification_finalization_evidence evidence
        `;
        assert.lengthOf(source, 1);
        assert.equal(source[0]!.finalizationStorage, "text");
        assert.equal(source[0]!.legacyGuards, 5);
        assert.deepStrictEqual(yield* first.sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
            Effect.provideService(SqlClient.SqlClient, first.sql),
          ),
          [[62, "AgentControlTaskVerificationFinalization"] as const],
        );
        assert.deepStrictEqual(
          yield* first.sql<{
            readonly bytes: string;
            readonly storage: string;
          }>`
            SELECT hex(CAST(finalization_json AS BLOB)) AS bytes,
              typeof(finalization_json) AS storage
            FROM main.agent_control_verification_finalization_evidence
          `,
          [{ bytes: source[0]!.finalizationBytes, storage: "text" }],
        );

        assert.equal(
          (yield* first.finalizer.processHandoff(source[0]!.handoffId))._tag,
          "Finalized",
        );
        const terminal = Option.getOrThrow(
          yield* first.states.get(AgentControlTaskId.make(source[0]!.taskId)),
        );
        assert.equal(terminal.status, "failed");
        assert.equal(terminal.stage, "verification");
        assert.equal(yield* Ref.get(first.publications), 1);
        const stored = yield* first.sql<{
          readonly documentStorage: string;
          readonly documentUdfStorage: string;
          readonly eventStorage: string;
          readonly payloadUdfStorage: string;
        }>`
          SELECT typeof(event.payload_json) AS "eventStorage",
            typeof(t3_task_verification_finalization_payload_storage(
              event.event_type, CAST(event.payload_json AS BLOB),
              CAST(event.metadata_json AS BLOB), event.event_id,
              event.stream_version, event.command_id
            )) AS "payloadUdfStorage",
            typeof(evidence.finalization_json) AS "documentStorage",
            typeof(t3_task_verification_finalization_document_storage(
              CAST(evidence.finalization_json AS BLOB), CAST(event.payload_json AS BLOB),
              event.event_id, event.stream_version, evidence.finalization_command_id,
              evidence.task_finalization_evidence_id, evidence.receipt_id,
              evidence.marker_id, evidence.finalization_fingerprint,
              evidence.project_id, evidence.task_id, evidence.verification_task_revision,
              evidence.previous_task_revision, evidence.github_intake_sequence,
              evidence.source_identity_fingerprint, evidence.task_source_event_id,
              evidence.task_source_event_sequence, evidence.task_source_event_stream_version
            )) AS "documentUdfStorage"
          FROM main.agent_control_task_verification_finalization_evidence evidence
          JOIN main.agent_control_events event ON event.event_id = evidence.task_event_id
        `;
        assert.deepStrictEqual(stored, [
          {
            documentStorage: "text",
            documentUdfStorage: "blob",
            eventStorage: "text",
            payloadUdfStorage: "blob",
          },
        ]);
        assert.deepStrictEqual(yield* first.sql`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* first.sql`PRAGMA integrity_check`, [
          { integrity_check: "ok" },
        ]);
        const beforeRestart = yield* finalizationCounts(first.sql);
        yield* Scope.close(firstScope, Exit.void);

        const restartScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(restartScope, Exit.void));
        const restart = yield* buildRuntime(filename, restartScope);
        const changesBeforeReplay = yield* restart.sql<{ readonly changes: number }>`
          SELECT total_changes() AS changes
        `;
        assert.equal(
          (yield* restart.finalizer.processHandoff(source[0]!.handoffId))._tag,
          "Replayed",
        );
        assert.deepStrictEqual(yield* finalizationCounts(restart.sql), beforeRestart);
        assert.equal(yield* Ref.get(restart.publications), 0);
        assert.deepStrictEqual(
          yield* restart.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          changesBeforeReplay,
        );
        assert.deepStrictEqual(yield* restart.sql`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* restart.sql`PRAGMA integrity_check`, [
          { integrity_check: "ok" },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live("maps every committed Verification disposition and publishes once after commit", () =>
  withDatabase("task-verification-finalizer-mapping-", (filename, runtime) =>
    Effect.gen(function* () {
      for (const table of [
        "agent_control_events",
        "agent_control_projection_state",
        "agent_control_task_states",
        "agent_control_verification_handoff_accepted",
        "agent_control_verification_finalization_evidence",
        "agent_control_verification_finalization_receipts",
        "agent_control_verification_finalization_markers",
        "agent_control_task_verification_finalization_evidence",
        "agent_control_task_verification_finalization_receipts",
        "agent_control_task_verification_finalization_publications",
        "agent_control_task_verification_finalization_markers",
      ]) {
        yield* runtime.sql.unsafe(`CREATE TEMP TABLE ${table}(shadow INTEGER)`);
      }
      const cases = [
        ["passed", "succeeded"],
        ["failed-verdict", "failed"],
        ["invalid-output", "failed"],
        ["delivery-failed", "failed"],
        ["interrupted", "cancelled"],
      ] as const;
      for (const [outcome, expectedStatus] of cases) {
        yield* seedTask(runtime, outcome);
        const source = seedCommittedVerificationFinalization(filename, outcome, outcome);
        const result = yield* runtime.finalizer.processHandoff(source.handoffId);
        assert.equal(result._tag, "Finalized");
        const state = Option.getOrThrow(yield* runtime.states.get(source.taskId));
        assert.equal(state.status, expectedStatus);
        assert.equal(state.stage, "verification");
        assert.equal(state.revision, 2);
      }
      for (const [outcome, expectedStatus] of [
        ["passed", "succeeded"],
        ["failed-verdict", "failed"],
        ["interrupted", "cancelled"],
      ] as const) {
        const taskId = AgentControlTaskId.make(`task-${outcome}`);
        const events = yield* runtime.events.readStream(taskId, 0, 500);
        const terminal = yield* loadRunOnceTerminalAuthority(
          runtime.sql,
          ProjectId.make(`task-finalizer-project-${outcome}`),
          AgentControlRunOnceId.make(`run-once-terminal-${outcome}`),
          taskId,
          events,
        );
        assert.isNotNull(terminal, outcome);
        assert.equal(terminal?.status, expectedStatus, outcome);
        assert.equal(terminal?.event.payload.status, expectedStatus, outcome);
      }
      assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
        { evidence: 5, events: 5, markers: 5, publications: 5, receipts: 5 },
      ]);
      assert.equal(yield* Ref.get(runtime.publications), 5);
      assert.lengthOf(yield* Ref.get(runtime.publishedEvents), 5);

      const invalidFollowup = yield* Effect.exit(runtime.sql`
        INSERT INTO main.agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        )
        SELECT event_id || '-invalid', aggregate_kind, stream_id, stream_version + 1, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        FROM main.agent_control_events
        WHERE event_type = 'agentControl.task.finalizedAfterVerification'
        ORDER BY sequence LIMIT 1
      `);
      assert.isTrue(Exit.isFailure(invalidFollowup));
      const immutableEvent = yield* Effect.exit(runtime.sql`
        UPDATE main.agent_control_events SET payload_json = payload_json || ' '
        WHERE event_type = 'agentControl.task.finalizedAfterVerification'
      `);
      assert.isTrue(Exit.isFailure(immutableEvent));
      const immutableProjection = yield* Effect.exit(runtime.sql`
        UPDATE main.agent_control_task_states SET status = 'failed'
        WHERE task_id = 'task-passed'
      `);
      assert.isTrue(Exit.isFailure(immutableProjection));
      const immutableEvidence = yield* Effect.exit(runtime.sql`
        UPDATE main.agent_control_task_verification_finalization_evidence
        SET finalization_fingerprint = ${"f".repeat(64)}
        WHERE task_id = 'task-passed'
      `);
      assert.isTrue(Exit.isFailure(immutableEvidence));
      const invalidPublicationRegression = yield* Effect.exit(runtime.sql`
        UPDATE main.agent_control_task_verification_finalization_publications
        SET status = 'claimed', revision = revision + 1, completed_at = NULL
        WHERE task_id = 'task-passed'
      `);
      assert.isTrue(Exit.isFailure(invalidPublicationRegression));
      const deletePublication = yield* Effect.exit(runtime.sql`
        DELETE FROM main.agent_control_task_verification_finalization_publications
        WHERE task_id = 'task-passed'
      `);
      assert.isTrue(Exit.isFailure(deletePublication));
      assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
        { evidence: 5, events: 5, markers: 5, publications: 5, receipts: 5 },
      ]);
    }),
  ),
);

it.live(
  "rejects non-INTEGER or divergent Evidence source coordinates before every follow-up write",
  () => {
    let hookCalls = 0;
    const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
    const hooks: AgentControlTaskVerificationFinalizerHooksShape = {
      beforeTransaction: hook,
      afterAuthoritativeRead: hook,
      afterTaskProjection: hook,
      afterEvidence: hook,
      afterReceipt: hook,
      beforeMarker: hook,
      afterCommit: hook,
      afterPublication: hook,
    };
    return withDatabase(
      "task-verification-finalizer-evidence-source-boundary-",
      (filename, runtime) =>
        Effect.gen(function* () {
          yield* seedTask(runtime, "evidence-source-boundary");
          const source = seedCommittedVerificationFinalization(
            filename,
            "evidence-source-boundary",
            "passed",
          );
          assert.equal(
            (yield* runtime.finalizer.processHandoff(source.handoffId))._tag,
            "Finalized",
          );
          const prepared = yield* loadPreparedTaskFinalizationEvidence(
            runtime.sql,
            source.handoffId,
          );
          const publicationsBefore = yield* Ref.get(runtime.publications);
          const publishedEventsBefore = yield* Ref.get(runtime.publishedEvents);
          const hooksBefore = hookCalls;

          yield* Effect.sync(() => {
            const database = new NodeSqlite.DatabaseSync(filename);
            try {
              database.exec("PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE");
              const tables = [
                "agent_control_task_verification_finalization_markers",
                "agent_control_task_verification_finalization_publications",
                "agent_control_task_verification_finalization_receipts",
                "agent_control_task_verification_finalization_evidence",
              ] as const;
              const triggers = database
                .prepare(
                  `SELECT name, sql FROM main.sqlite_schema
                   WHERE type = 'trigger' AND tbl_name IN (?, ?, ?, ?)
                     AND sql LIKE '%BEFORE DELETE%'`,
                )
                .all(...tables) as unknown as ReadonlyArray<{
                readonly name: string;
                readonly sql: string;
              }>;
              assert.lengthOf(triggers, 4);
              for (const trigger of triggers) {
                database.exec(`DROP TRIGGER main."${trigger.name.replaceAll('"', '""')}"`);
              }
              for (const table of tables) {
                database
                  .prepare(`DELETE FROM main.${table} WHERE handoff_id = ?`)
                  .run(source.handoffId);
              }
              for (const trigger of triggers) database.exec(trigger.sql);
              database.exec("COMMIT");
            } catch (cause) {
              if (database.isTransaction) database.exec("ROLLBACK");
              throw cause;
            } finally {
              database.close();
            }
          });

          const before = yield* taskFinalizationAuthoritySnapshot(
            runtime.sql,
            source.handoffId,
            source.taskId,
          );
          assert.deepStrictEqual(before.evidence, []);
          assert.deepStrictEqual(before.receipts, []);
          assert.deepStrictEqual(before.markers, []);
          assert.deepStrictEqual(before.publication, []);
          for (const overrides of [
            {
              previousTaskRevision: Buffer.from(
                String(prepared.payload.previousTaskRevision),
                "utf8",
              ),
            },
            { previousTaskRevision: prepared.payload.previousTaskRevision + 1 },
            {
              taskSourceEventSequence: Buffer.from(
                String(prepared.payload.taskSourceEventSequence),
                "utf8",
              ),
            },
            { taskSourceEventSequence: prepared.payload.taskSourceEventSequence + 1 },
          ] as const) {
            const rejected = yield* Effect.exit(
              insertPreparedTaskFinalizationEvidence(runtime.sql, prepared, overrides),
            );
            assert.isTrue(Exit.isFailure(rejected));
            assert.deepStrictEqual(
              yield* taskFinalizationAuthoritySnapshot(
                runtime.sql,
                source.handoffId,
                source.taskId,
              ),
              before,
            );
            assert.equal(yield* Ref.get(runtime.publications), publicationsBefore);
            assert.deepStrictEqual(yield* Ref.get(runtime.publishedEvents), publishedEventsBefore);
            assert.equal(hookCalls, hooksBefore);
          }

          yield* runtime.sql.withTransaction(
            Effect.gen(function* () {
              yield* insertPreparedTaskFinalizationEvidence(runtime.sql, prepared);
              const payload = prepared.payload;
              yield* runtime.sql`
                INSERT INTO main.agent_control_task_verification_finalization_receipts (
                  receipt_id, marker_id, task_finalization_evidence_id,
                  finalization_command_id, finalization_fingerprint,
                  verification_marker_id, handoff_id, task_id,
                  task_event_id, task_event_sequence, task_event_stream_version,
                  status, accepted_at
                ) VALUES (
                  ${prepared.receiptId}, ${prepared.markerId},
                  ${payload.taskFinalizationEvidenceId}, ${prepared.commandId},
                  ${prepared.finalizationFingerprint},
                  ${payload.verificationFinalizationMarkerId}, ${payload.handoffId},
                  ${payload.taskId}, ${prepared.eventId}, ${prepared.eventSequence},
                  ${prepared.eventStreamVersion}, 'accepted', ${payload.finalizedAt}
                )
              `;
              yield* runtime.sql`
                INSERT INTO main.agent_control_task_verification_finalization_publications (
                  handoff_id, marker_id, task_finalization_evidence_id, task_id,
                  task_event_id, task_event_stream_version, publication_owner_id,
                  status, revision, claim_fence, created_at, claimed_at,
                  lease_expires_at, completed_at
                ) VALUES (
                  ${payload.handoffId}, ${prepared.markerId},
                  ${payload.taskFinalizationEvidenceId}, ${payload.taskId},
                  ${prepared.eventId}, ${prepared.eventStreamVersion}, NULL,
                  'pending', 1, 0, ${payload.finalizedAt}, NULL, NULL, NULL
                )
              `;
              yield* runtime.sql`
                INSERT INTO main.agent_control_task_verification_finalization_markers (
                  marker_id, marker_fingerprint, receipt_id,
                  task_finalization_evidence_id, finalization_command_id,
                  finalization_fingerprint, verification_marker_id, handoff_id,
                  task_id, task_event_id, task_event_sequence,
                  task_event_stream_version, committed_at
                ) VALUES (
                  ${prepared.markerId}, ${prepared.markerFingerprint}, ${prepared.receiptId},
                  ${payload.taskFinalizationEvidenceId}, ${prepared.commandId},
                  ${prepared.finalizationFingerprint},
                  ${payload.verificationFinalizationMarkerId}, ${payload.handoffId},
                  ${payload.taskId}, ${prepared.eventId}, ${prepared.eventSequence},
                  ${prepared.eventStreamVersion}, ${payload.finalizedAt}
                )
              `;
            }),
          );
          assert.deepStrictEqual(
            yield* runtime.sql<{
              readonly previousRevisionStorage: string;
              readonly sourceSequenceStorage: string;
            }>`
              SELECT typeof(previous_task_revision) AS "previousRevisionStorage",
                typeof(task_source_event_sequence) AS "sourceSequenceStorage"
              FROM main.agent_control_task_verification_finalization_evidence
              WHERE handoff_id = ${source.handoffId}
            `,
            [{ previousRevisionStorage: "integer", sourceSequenceStorage: "integer" }],
          );
          assert.equal(yield* Ref.get(runtime.publications), publicationsBefore);
          assert.deepStrictEqual(yield* Ref.get(runtime.publishedEvents), publishedEventsBefore);
          assert.equal(hookCalls, hooksBefore);
        }),
      hooks,
    );
  },
);

it.live("rejects crossed or non-TEXT Marker receipt authority without DML or publication", () => {
  let hookCalls = 0;
  const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
  const countingHooks: AgentControlTaskVerificationFinalizerHooksShape = {
    beforeTransaction: hook,
    afterAuthoritativeRead: hook,
    afterTaskProjection: hook,
    afterEvidence: hook,
    afterReceipt: hook,
    beforeMarker: hook,
    afterCommit: hook,
    afterPublication: hook,
  };
  return withDatabase(
    "task-verification-finalizer-marker-receipt-boundary-",
    (filename, runtime) =>
      Effect.gen(function* () {
        const sources = [] as Array<ReturnType<typeof seedCommittedVerificationFinalization>>;
        for (const suffix of ["marker-receipt-a", "marker-receipt-b"] as const) {
          yield* seedTask(runtime, suffix);
          const source = seedCommittedVerificationFinalization(filename, suffix, "passed");
          sources.push(source);
          assert.equal(
            (yield* runtime.finalizer.processHandoff(source.handoffId))._tag,
            "Finalized",
          );
        }
        const markerRows = yield* runtime.sql<{
          readonly committedAt: string;
          readonly finalizationCommandId: string;
          readonly finalizationFingerprint: string;
          readonly handoffId: string;
          readonly markerFingerprint: string;
          readonly markerId: string;
          readonly receiptId: string;
          readonly taskEventId: string;
          readonly taskEventSequence: number;
          readonly taskEventStreamVersion: number;
          readonly taskFinalizationEvidenceId: string;
          readonly taskId: string;
          readonly verificationMarkerId: string;
        }>`
          SELECT marker_id AS "markerId", marker_fingerprint AS "markerFingerprint",
            receipt_id AS "receiptId",
            task_finalization_evidence_id AS "taskFinalizationEvidenceId",
            finalization_command_id AS "finalizationCommandId",
            finalization_fingerprint AS "finalizationFingerprint",
            verification_marker_id AS "verificationMarkerId", handoff_id AS "handoffId",
            task_id AS "taskId", task_event_id AS "taskEventId",
            task_event_sequence AS "taskEventSequence",
            task_event_stream_version AS "taskEventStreamVersion",
            committed_at AS "committedAt"
          FROM main.agent_control_task_verification_finalization_markers
          WHERE handoff_id IN (${sources[0]!.handoffId}, ${sources[1]!.handoffId})
          ORDER BY handoff_id
        `;
        assert.lengthOf(markerRows, 2);
        const committedBefore = yield* Effect.all(
          sources.map((source) =>
            taskFinalizationAuthoritySnapshot(runtime.sql, source.handoffId, source.taskId),
          ),
        );
        const publicationsBefore = yield* Ref.get(runtime.publications);
        const publishedEventsBefore = yield* Ref.get(runtime.publishedEvents);
        const hooksBefore = hookCalls;

        const rolledBack = yield* Effect.exit(
          runtime.sql.withTransaction(
            Effect.gen(function* () {
              const triggerRows = yield* runtime.sql<{
                readonly name: string;
                readonly sql: string;
              }>`
                SELECT name, sql FROM main.sqlite_schema
                WHERE type = 'trigger' AND name IN (
                  'agent_control_task_verification_finalization_markers_no_delete',
                  'agent_control_task_verification_finalization_publication_update_validate'
                )
                ORDER BY name
              `;
              assert.lengthOf(triggerRows, 2);
              const triggerSql = Object.fromEntries(
                triggerRows.map((row) => [row.name, row.sql] as const),
              );
              yield* runtime.sql.unsafe(
                `DROP TRIGGER main.agent_control_task_verification_finalization_publication_update_validate`,
              ).unprepared;
              yield* runtime.sql`
                UPDATE main.agent_control_task_verification_finalization_publications
                SET publication_owner_id = NULL, status = 'pending', revision = 1,
                  claim_fence = 0, claimed_at = NULL, lease_expires_at = NULL,
                  completed_at = NULL
                WHERE handoff_id IN (${sources[0]!.handoffId}, ${sources[1]!.handoffId})
              `;
              yield* runtime.sql.unsafe(
                triggerSql.agent_control_task_verification_finalization_publication_update_validate!,
              ).unprepared;
              yield* runtime.sql.unsafe(
                `DROP TRIGGER main.agent_control_task_verification_finalization_markers_no_delete`,
              ).unprepared;
              yield* runtime.sql`
                DELETE FROM main.agent_control_task_verification_finalization_markers
                WHERE handoff_id IN (${sources[0]!.handoffId}, ${sources[1]!.handoffId})
              `;
              yield* runtime.sql.unsafe(
                triggerSql.agent_control_task_verification_finalization_markers_no_delete!,
              ).unprepared;

              const preparedBefore = yield* Effect.all(
                sources.map((source) =>
                  taskFinalizationAuthoritySnapshot(runtime.sql, source.handoffId, source.taskId),
                ),
              );
              for (const prepared of preparedBefore) {
                assert.lengthOf(prepared.evidence, 1);
                assert.lengthOf(prepared.receipts, 1);
                assert.lengthOf(prepared.publication, 1);
                assert.deepStrictEqual(prepared.markers, []);
                assert.equal(prepared.publication[0]!.status, "pending");
                assert.equal(prepared.publication[0]!.revision, 1);
              }

              const marker = markerRows[0]!;
              const insertMarker = (receiptId: unknown) =>
                runtime.sql`
                  INSERT INTO main.agent_control_task_verification_finalization_markers (
                    marker_id, marker_fingerprint, receipt_id,
                    task_finalization_evidence_id, finalization_command_id,
                    finalization_fingerprint, verification_marker_id, handoff_id,
                    task_id, task_event_id, task_event_sequence,
                    task_event_stream_version, committed_at
                  ) VALUES (
                    ${marker.markerId}, ${marker.markerFingerprint}, ${receiptId},
                    ${marker.taskFinalizationEvidenceId}, ${marker.finalizationCommandId},
                    ${marker.finalizationFingerprint}, ${marker.verificationMarkerId},
                    ${marker.handoffId}, ${marker.taskId}, ${marker.taskEventId},
                    ${marker.taskEventSequence}, ${marker.taskEventStreamVersion},
                    ${marker.committedAt}
                  )
                `;
              for (const receiptId of [
                markerRows[1]!.receiptId,
                Buffer.from(marker.receiptId, "utf8"),
              ]) {
                const rejected = yield* Effect.exit(insertMarker(receiptId));
                assert.isTrue(Exit.isFailure(rejected));
                assert.deepStrictEqual(
                  yield* Effect.all(
                    sources.map((source) =>
                      taskFinalizationAuthoritySnapshot(
                        runtime.sql,
                        source.handoffId,
                        source.taskId,
                      ),
                    ),
                  ),
                  preparedBefore,
                );
                assert.equal(yield* Ref.get(runtime.publications), publicationsBefore);
                assert.deepStrictEqual(
                  yield* Ref.get(runtime.publishedEvents),
                  publishedEventsBefore,
                );
                assert.equal(hookCalls, hooksBefore);
              }
              return yield* Effect.fail("expected Marker boundary rollback" as const);
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(rolledBack));
        const committedAfter = yield* Effect.all(
          sources.map((source) =>
            taskFinalizationAuthoritySnapshot(runtime.sql, source.handoffId, source.taskId),
          ),
        );
        for (let index = 0; index < committedBefore.length; index++) {
          assert.deepStrictEqual(committedAfter[index]!.evidence, committedBefore[index]!.evidence);
          assert.deepStrictEqual(committedAfter[index]!.receipts, committedBefore[index]!.receipts);
          assert.deepStrictEqual(committedAfter[index]!.markers, committedBefore[index]!.markers);
          assert.deepStrictEqual(
            committedAfter[index]!.publication,
            committedBefore[index]!.publication,
          );
          assert.deepStrictEqual(
            committedAfter[index]!.taskEvents,
            committedBefore[index]!.taskEvents,
          );
          assert.deepStrictEqual(
            committedAfter[index]!.projection,
            committedBefore[index]!.projection,
          );
          assert.deepStrictEqual(
            committedAfter[index]!.revisionCounters,
            committedBefore[index]!.revisionCounters,
          );
        }
        assert.equal(yield* Ref.get(runtime.publications), publicationsBefore);
        assert.deepStrictEqual(yield* Ref.get(runtime.publishedEvents), publishedEventsBefore);
        assert.equal(hookCalls, hooksBefore);
      }),
    countingHooks,
  );
});

it.live("replays without DML or hooks and fails closed on divergent committed authority", () => {
  let hookCalls = 0;
  const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
  const countingHooks: AgentControlTaskVerificationFinalizerHooksShape = {
    beforeTransaction: hook,
    afterAuthoritativeRead: hook,
    afterTaskProjection: hook,
    afterEvidence: hook,
    afterReceipt: hook,
    beforeMarker: hook,
    afterCommit: hook,
    afterPublication: hook,
  };
  return withDatabase(
    "task-verification-finalizer-replay-",
    (filename, runtime) =>
      Effect.gen(function* () {
        yield* seedTask(runtime, "replay");
        const source = seedCommittedVerificationFinalization(filename, "replay", "passed");
        assert.equal((yield* runtime.finalizer.processHandoff(source.handoffId))._tag, "Finalized");
        const before = yield* finalizationCounts(runtime.sql);
        const publications = yield* Ref.get(runtime.publications);
        const hooksAfterFinalization = hookCalls;
        const changesBefore = yield* runtime.sql<{ readonly changes: number }>`
        SELECT total_changes() AS changes
      `;
        assert.equal((yield* runtime.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), before);
        assert.equal(yield* Ref.get(runtime.publications), publications);
        assert.equal(hookCalls, hooksAfterFinalization);
        assert.deepStrictEqual(
          yield* runtime.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          changesBefore,
        );

        const database = new NodeSqlite.DatabaseSync(filename);
        try {
          database.exec(
            "DROP TRIGGER main.agent_control_verification_finalization_markers_no_update",
          );
          database
            .prepare(
              `UPDATE main.agent_control_verification_finalization_markers
             SET marker_fingerprint = ? WHERE marker_id = ?`,
            )
            .run("f".repeat(64), source.markerId);
        } finally {
          database.close();
        }
        const divergent = yield* Effect.result(runtime.finalizer.processHandoff(source.handoffId));
        assert.equal(divergent._tag, "Failure");
        if (divergent._tag === "Failure")
          assert.equal(divergent.failure.reason, "authority-conflict");
        assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), before);
        assert.equal(yield* Ref.get(runtime.publications), publications);
      }),
    countingHooks,
  );
});

it.live("strictly closes every replay Evidence, Receipt, and Marker coordinate before DML", () => {
  let hookCalls = 0;
  const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
  const countingHooks: AgentControlTaskVerificationFinalizerHooksShape = {
    beforeTransaction: hook,
    afterAuthoritativeRead: hook,
    afterTaskProjection: hook,
    afterEvidence: hook,
    afterReceipt: hook,
    beforeMarker: hook,
    afterCommit: hook,
    afterPublication: hook,
  };
  return withDatabase(
    "task-verification-finalizer-replay-storage-",
    (filename, runtime) =>
      Effect.gen(function* () {
        const cases = [
          {
            suffix: "replay-evidence-sequence-blob",
            table: "agent_control_task_verification_finalization_evidence",
            expectedOperation: "decode-replay-row",
            expectedReason: "authority-conflict",
            mutate: (database: NodeSqlite.DatabaseSync, handoffId: string) => {
              const result = database
                .prepare(
                  `UPDATE main.agent_control_task_verification_finalization_evidence
                     SET task_event_sequence = ? WHERE handoff_id = ?`,
                )
                .run(new Uint8Array([0x31, 0x32]), handoffId);
              assert.equal(Number(result.changes), 1);
            },
          },
          {
            suffix: "replay-receipt-sequence-divergent",
            table: "agent_control_task_verification_finalization_receipts",
            expectedOperation: "compare-replay",
            expectedReason: "identity-mismatch",
            mutate: (database: NodeSqlite.DatabaseSync, handoffId: string) => {
              const result = database
                .prepare(
                  `UPDATE main.agent_control_task_verification_finalization_receipts
                     SET task_event_sequence = task_event_sequence + 100000
                     WHERE handoff_id = ?`,
                )
                .run(handoffId);
              assert.equal(Number(result.changes), 1);
            },
          },
          {
            suffix: "replay-receipt-sequence-blob",
            table: "agent_control_task_verification_finalization_receipts",
            expectedOperation: "decode-replay-row",
            expectedReason: "authority-conflict",
            mutate: (database: NodeSqlite.DatabaseSync, handoffId: string) => {
              const result = database
                .prepare(
                  `UPDATE main.agent_control_task_verification_finalization_receipts
                     SET task_event_sequence = ? WHERE handoff_id = ?`,
                )
                .run(new Uint8Array([0x31, 0x32]), handoffId);
              assert.equal(Number(result.changes), 1);
            },
          },
          {
            suffix: "replay-marker-event-id-divergent",
            table: "agent_control_task_verification_finalization_markers",
            expectedOperation: "compare-replay",
            expectedReason: "identity-mismatch",
            mutate: (database: NodeSqlite.DatabaseSync, handoffId: string) => {
              const result = database
                .prepare(
                  `UPDATE main.agent_control_task_verification_finalization_markers
                     SET task_event_id = task_event_id || '-corrupt' WHERE handoff_id = ?`,
                )
                .run(handoffId);
              assert.equal(Number(result.changes), 1);
            },
          },
          {
            suffix: "replay-marker-sequence-blob",
            table: "agent_control_task_verification_finalization_markers",
            expectedOperation: "decode-replay-row",
            expectedReason: "authority-conflict",
            mutate: (database: NodeSqlite.DatabaseSync, handoffId: string) => {
              const result = database
                .prepare(
                  `UPDATE main.agent_control_task_verification_finalization_markers
                     SET task_event_sequence = ? WHERE handoff_id = ?`,
                )
                .run(new Uint8Array([0x31, 0x32]), handoffId);
              assert.equal(Number(result.changes), 1);
            },
          },
        ] as const;

        const sources = new Map<string, ReturnType<typeof seedCommittedVerificationFinalization>>();
        for (const testCase of cases) {
          yield* seedTask(runtime, testCase.suffix);
          const source = seedCommittedVerificationFinalization(filename, testCase.suffix, "passed");
          sources.set(testCase.suffix, source);
          assert.equal(
            (yield* runtime.finalizer.processHandoff(source.handoffId))._tag,
            "Finalized",
          );
        }

        for (const testCase of cases) {
          const source = sources.get(testCase.suffix)!;
          const database = new NodeSqlite.DatabaseSync(filename);
          try {
            withUpdateGuardsDisabled(database, testCase.table, () => {
              testCase.mutate(database, source.handoffId);
            });
            const storage = database
              .prepare(
                `SELECT typeof(task_event_sequence) AS sequence_storage_class,
                    typeof(task_event_id) AS event_id_storage_class
                   FROM main."${testCase.table}" WHERE handoff_id = ?`,
              )
              .get(source.handoffId) as {
              readonly event_id_storage_class: string;
              readonly sequence_storage_class: string;
            };
            assert.equal(
              storage.sequence_storage_class,
              testCase.suffix.endsWith("-blob") ? "blob" : "integer",
            );
            assert.equal(storage.event_id_storage_class, "text");
          } finally {
            database.close();
          }

          const authorityBefore = yield* taskFinalizationAuthoritySnapshot(
            runtime.sql,
            source.handoffId,
            source.taskId,
          );
          const hooksBefore = hookCalls;
          const publicationsBefore = yield* Ref.get(runtime.publications);
          const publishedEventsBefore = yield* Ref.get(runtime.publishedEvents);
          const replay = yield* Effect.result(runtime.finalizer.processHandoff(source.handoffId));
          assert.equal(replay._tag, "Failure");
          if (replay._tag === "Failure") {
            assert.equal(replay.failure.reason, testCase.expectedReason);
            assert.equal(replay.failure.operation, testCase.expectedOperation);
          }
          assert.deepStrictEqual(
            yield* taskFinalizationAuthoritySnapshot(runtime.sql, source.handoffId, source.taskId),
            authorityBefore,
          );
          assert.equal(hookCalls, hooksBefore);
          assert.equal(yield* Ref.get(runtime.publications), publicationsBefore);
          assert.deepStrictEqual(yield* Ref.get(runtime.publishedEvents), publishedEventsBefore);
        }
      }),
    countingHooks,
  );
});

it.live(
  "replays current task history and rejects non-mirrored projection or historical divergence before DML",
  () => {
    let hookCalls = 0;
    const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
    const countingHooks: AgentControlTaskVerificationFinalizerHooksShape = {
      beforeTransaction: hook,
      afterAuthoritativeRead: hook,
      afterTaskProjection: hook,
      afterEvidence: hook,
      afterReceipt: hook,
      beforeMarker: hook,
      afterCommit: hook,
      afterPublication: hook,
    };
    return withDatabase(
      "task-verification-finalizer-current-authority-",
      (filename, runtime) =>
        Effect.gen(function* () {
          for (const suffix of ["projection-divergent", "history-divergent"] as const) {
            yield* seedTask(runtime, suffix);
            seedCommittedVerificationFinalization(filename, suffix, "passed");
          }
          const database = new NodeSqlite.DatabaseSync(filename);
          try {
            database.exec("PRAGMA ignore_check_constraints = ON");
            withUpdateGuardsDisabled(database, "agent_control_events", () =>
              withUpdateGuardsDisabled(database, "agent_control_task_states", () => {
                database
                  .prepare(
                    `UPDATE main.agent_control_task_states
                   SET state_json = json_set(
                     state_json,
                     '$.sourceSnapshot.title', 'projection-only-title',
                     '$.sourceSnapshot.body', 'projection-only-body',
                     '$.sourceSnapshot.ready', json('false'),
                     '$.sourceSnapshot.eligible', json('false'),
                     '$.sourceSnapshot.eligibilityReason', 'ready-inactive'
                   )
                   WHERE task_id = ?`,
                  )
                  .run("task-projection-divergent");
                database
                  .prepare(
                    `UPDATE main.agent_control_events
                   SET payload_json = json_set(
                     payload_json,
                     '$.sourceSnapshot.title', 'history-only-title',
                     '$.sourceSnapshot.body', 'history-only-body'
                   )
                   WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                  )
                  .run("task-history-divergent");
              }),
            );
          } finally {
            database.close();
          }

          const changesBefore = yield* runtime.sql<{ readonly changes: number }>`
          SELECT total_changes() AS changes
        `;
          for (const suffix of ["projection-divergent", "history-divergent"] as const) {
            const result = yield* Effect.result(
              runtime.finalizer.processHandoff(`handoff-${suffix}`),
            );
            assert.equal(result._tag, "Failure", suffix);
            if (result._tag === "Failure") {
              assert.equal(result.failure.reason, "authority-conflict", suffix);
            }
          }
          assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
            { evidence: 0, events: 0, markers: 0, publications: 0, receipts: 0 },
          ]);
          assert.equal(yield* Ref.get(runtime.publications), 0);
          assert.equal(hookCalls, 0);
          assert.deepStrictEqual(
            yield* runtime.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
            changesBefore,
          );
        }),
      countingHooks,
    );
  },
);

it.live(
  "rejects duplicate, excess, and non-TEXT task history or projection authority before DML",
  () => {
    let hookCalls = 0;
    const hook = () => Effect.sync(() => hookCalls++).pipe(Effect.asVoid);
    const countingHooks: AgentControlTaskVerificationFinalizerHooksShape = {
      beforeTransaction: hook,
      afterAuthoritativeRead: hook,
      afterTaskProjection: hook,
      afterEvidence: hook,
      afterReceipt: hook,
      beforeMarker: hook,
      afterCommit: hook,
      afterPublication: hook,
    };
    return withDatabase(
      "task-verification-finalizer-raw-authority-",
      (filename, runtime) =>
        Effect.gen(function* () {
          const cases = [
            "payload-duplicate-identical",
            "payload-duplicate-divergent",
            "payload-excess",
            "payload-blob",
            "metadata-duplicate",
            "metadata-excess",
            "metadata-blob",
            "projection-duplicate-identical",
            "projection-duplicate-divergent",
            "projection-excess",
            "projection-blob",
            "projection-mirror-blob",
          ] as const;
          for (const suffix of cases) {
            yield* seedTask(runtime, suffix);
            seedCommittedVerificationFinalization(filename, suffix, "passed");
          }
          const database = new NodeSqlite.DatabaseSync(filename);
          try {
            database.exec("PRAGMA ignore_check_constraints = ON");
            withUpdateGuardsDisabled(database, "agent_control_events", () =>
              withUpdateGuardsDisabled(database, "agent_control_task_states", () => {
                const eventSource = (suffix: string) =>
                  (
                    database
                      .prepare(
                        `SELECT payload_json AS source FROM main.agent_control_events
                     WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                      )
                      .get(`task-${suffix}`) as { readonly source: string }
                  ).source;
                const stateSource = (suffix: string) =>
                  (
                    database
                      .prepare(
                        `SELECT state_json AS source FROM main.agent_control_task_states
                     WHERE task_id = ?`,
                      )
                      .get(`task-${suffix}`) as { readonly source: string }
                  ).source;
                const replaceTitle = (source: string, suffix: string, duplicate: string) => {
                  const title = `"title":"Task ${suffix}"`;
                  const replaced = source.replace(
                    title,
                    `${title},"title":${JSON.stringify(duplicate)}`,
                  );
                  assert.notEqual(replaced, source, suffix);
                  return replaced;
                };
                const updateEventPayload = (suffix: string, value: string | Uint8Array) =>
                  database
                    .prepare(
                      `UPDATE main.agent_control_events SET payload_json = ?
                     WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                    )
                    .run(value, `task-${suffix}`);
                const updateProjection = (suffix: string, value: string | Uint8Array) =>
                  database
                    .prepare(
                      `UPDATE main.agent_control_task_states SET state_json = ? WHERE task_id = ?`,
                    )
                    .run(value, `task-${suffix}`);

                updateEventPayload(
                  "payload-duplicate-identical",
                  replaceTitle(
                    eventSource("payload-duplicate-identical"),
                    "payload-duplicate-identical",
                    "Task payload-duplicate-identical",
                  ),
                );
                updateEventPayload(
                  "payload-duplicate-divergent",
                  replaceTitle(
                    eventSource("payload-duplicate-divergent"),
                    "payload-duplicate-divergent",
                    "divergent-title",
                  ),
                );
                const excessPayload = eventSource("payload-excess");
                updateEventPayload(
                  "payload-excess",
                  `${excessPayload.slice(0, -1)},"unexpected":true}`,
                );
                updateEventPayload(
                  "payload-blob",
                  Buffer.from(eventSource("payload-blob"), "utf8"),
                );
                database
                  .prepare(
                    `UPDATE main.agent_control_events SET metadata_json = ?
                   WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                  )
                  .run('{"schemaVersion":1,"schemaVersion":1}', "task-metadata-duplicate");
                database
                  .prepare(
                    `UPDATE main.agent_control_events SET metadata_json = ?
                   WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                  )
                  .run('{"schemaVersion":1,"unexpected":true}', "task-metadata-excess");
                database
                  .prepare(
                    `UPDATE main.agent_control_events SET metadata_json = ?
                   WHERE aggregate_kind = 'task' AND stream_id = ? AND stream_version = 1`,
                  )
                  .run(Buffer.from('{"schemaVersion":1}', "utf8"), "task-metadata-blob");
                updateProjection(
                  "projection-duplicate-identical",
                  replaceTitle(
                    stateSource("projection-duplicate-identical"),
                    "projection-duplicate-identical",
                    "Task projection-duplicate-identical",
                  ),
                );
                updateProjection(
                  "projection-duplicate-divergent",
                  replaceTitle(
                    stateSource("projection-duplicate-divergent"),
                    "projection-duplicate-divergent",
                    "divergent-title",
                  ),
                );
                const excessState = stateSource("projection-excess");
                updateProjection(
                  "projection-excess",
                  `${excessState.slice(0, -1)},"unexpected":true}`,
                );
                updateProjection(
                  "projection-blob",
                  Buffer.from(stateSource("projection-blob"), "utf8"),
                );
                database
                  .prepare(
                    `UPDATE main.agent_control_task_states
                   SET issue_url = CAST(issue_url AS BLOB) WHERE task_id = ?`,
                  )
                  .run("task-projection-mirror-blob");
              }),
            );
          } finally {
            database.close();
          }

          const changesBefore = yield* runtime.sql<{ readonly changes: number }>`
          SELECT total_changes() AS changes
        `;
          for (const suffix of cases) {
            const result = yield* Effect.result(
              runtime.finalizer.processHandoff(`handoff-${suffix}`),
            );
            assert.equal(result._tag, "Failure", suffix);
            if (result._tag === "Failure") {
              assert.equal(result.failure.reason, "authority-conflict", suffix);
            }
          }
          assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
            { evidence: 0, events: 0, markers: 0, publications: 0, receipts: 0 },
          ]);
          assert.equal(yield* Ref.get(runtime.publications), 0);
          assert.equal(hookCalls, 0);
          assert.deepStrictEqual(
            yield* runtime.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
            changesBefore,
          );
        }),
      countingHooks,
    );
  },
);

it.live("rolls back Evidence and projection failures, then retries without loser publication", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "task-verification-finalizer-rollback-",
      });
      const filename = `${directory}/state.sqlite`;
      const setupScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
      const setup = yield* buildRuntime(filename, setupScope);
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, setup.sql),
      );
      yield* seedTask(setup, "rollback");
      const source = seedCommittedVerificationFinalization(filename, "rollback", "passed");

      const failingScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(failingScope, Exit.void));
      const failing = yield* buildRuntime(filename, failingScope, {
        ...defaultHooks,
        afterEvidence: () => Effect.die(new Error("injected after-evidence failure")),
      });
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(failing.finalizer.processHandoff(source.handoffId))),
      );
      assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
        { evidence: 0, events: 0, markers: 0, publications: 0, receipts: 0 },
      ]);
      assert.equal(yield* Ref.get(failing.publications), 0);
      const stateBeforeRetry = Option.getOrThrow(yield* setup.states.get(source.taskId));
      assert.equal(stateBeforeRetry.revision, 1);
      assert.equal(stateBeforeRetry.stage, "intake");

      assert.equal((yield* setup.finalizer.processHandoff(source.handoffId))._tag, "Finalized");
      assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
        { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
      ]);
      assert.equal(yield* Ref.get(setup.publications), 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "publishes a native post-commit winner exactly once and preserves the original defect",
  () =>
    withDatabase("task-verification-finalizer-native-post-commit-", (filename, runtime) =>
      Effect.gen(function* () {
        yield* seedTask(runtime, "native-post-commit");
        const source = seedCommittedVerificationFinalization(
          filename,
          "native-post-commit",
          "passed",
        );
        const defect = new Error("task-finalizer-native-post-commit-defect");
        const failed = yield* Effect.exit(
          runtime.finalizer.processHandoff(source.handoffId).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, {
              afterAnyCommitBeforeReturn: () => Effect.void,
              afterCommitBeforeReturn: () => Effect.die(defect),
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(failed));
        if (Exit.isFailure(failed)) {
          assert.isTrue(Cause.hasDies(failed.cause));
          assert.include(Cause.pretty(failed.cause), defect.message);
        }
        assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
          { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
        ]);
        assert.equal(yield* Ref.get(runtime.publications), 1);
        assert.equal((yield* runtime.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.equal(yield* Ref.get(runtime.publications), 1);
      }),
    ),
);

it.live(
  "publishes through afterCommit defects, preserves each cause, and accepts a later candidate",
  () => {
    const hooks: AgentControlTaskVerificationFinalizerHooksShape = {
      ...defaultHooks,
      afterCommit: (handoffId) =>
        Effect.die(new Error(`task-finalizer-after-commit-defect:${handoffId}`)),
    };
    return withDatabase(
      "task-verification-finalizer-after-commit-defect-",
      (filename, runtime) =>
        Effect.gen(function* () {
          for (const suffix of ["after-commit-a", "after-commit-b"] as const) {
            yield* seedTask(runtime, suffix);
            const source = seedCommittedVerificationFinalization(filename, suffix, "passed");
            const failed = yield* Effect.exit(runtime.finalizer.processHandoff(source.handoffId));
            assert.isTrue(Exit.isFailure(failed), suffix);
            if (Exit.isFailure(failed)) {
              assert.isTrue(Cause.hasDies(failed.cause), suffix);
              assert.include(Cause.pretty(failed.cause), source.handoffId, suffix);
            }
          }
          assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
            { evidence: 2, events: 2, markers: 2, publications: 2, receipts: 2 },
          ]);
          assert.equal(yield* Ref.get(runtime.publications), 2);
          assert.equal(
            (yield* runtime.finalizer.processHandoff("handoff-after-commit-a"))._tag,
            "Replayed",
          );
          assert.equal(yield* Ref.get(runtime.publications), 2);
        }),
      hooks,
    );
  },
);

it.live("publishes after a real Fiber interrupt at the afterCommit hook", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "task-verification-finalizer-after-commit-interrupt-",
      });
      const filename = `${directory}/state.sqlite`;
      const reached = yield* Deferred.make<void>();
      const runtimeScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
      const runtime = yield* buildRuntime(filename, runtimeScope, {
        ...defaultHooks,
        afterCommit: () => Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never)),
      });
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, runtime.sql),
      );
      yield* seedTask(runtime, "after-commit-interrupt");
      const source = seedCommittedVerificationFinalization(
        filename,
        "after-commit-interrupt",
        "passed",
      );
      const fiber = yield* Effect.forkScoped(runtime.finalizer.processHandoff(source.handoffId));
      yield* Deferred.await(reached);
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(interrupted));
      if (Exit.isFailure(interrupted)) {
        assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
      }
      assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
        { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
      ]);
      assert.equal(yield* Ref.get(runtime.publications), 1);
      assert.equal((yield* runtime.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
      assert.equal(yield* Ref.get(runtime.publications), 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("recovers committed markers by deterministic keyset pages after restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "task-verification-finalizer-recovery-",
      });
      const filename = `${directory}/state.sqlite`;
      const firstScope = yield* Scope.make("sequential");
      const first = yield* buildRuntime(filename, firstScope);
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, first.sql),
      );
      for (const suffix of ["recovery-a", "recovery-b", "recovery-c"] as const) {
        yield* seedTask(first, suffix);
        seedCommittedVerificationFinalization(filename, suffix, "passed");
      }
      yield* Scope.close(firstScope, Exit.void);

      const restartScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(restartScope, Exit.void));
      const restart = yield* buildRuntime(filename, restartScope, {
        ...defaultHooks,
        recoveryPageSize: 1,
      });
      yield* restart.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(restart.sql), [
        { evidence: 3, events: 3, markers: 3, publications: 3, receipts: 3 },
      ]);
      assert.equal(yield* Ref.get(restart.publications), 3);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "skips completed publication and recovers one dead claim after restart without duplicate replay",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse("2026-08-30T11:00:00.000Z"));
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-publication-restart-",
        });
        const filename = `${directory}/state.sqlite`;
        const firstScope = yield* Scope.make("sequential");
        const first = yield* buildRuntime(filename, firstScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, first.sql),
        );
        yield* seedTask(first, "publication-restart");
        const source = seedCommittedVerificationFinalization(
          filename,
          "publication-restart",
          "passed",
        );
        assert.equal((yield* first.finalizer.processHandoff(source.handoffId))._tag, "Finalized");
        assert.equal(yield* Ref.get(first.publications), 1);
        assert.deepStrictEqual(
          yield* first.sql<{ readonly status: string }>`
            SELECT status FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          [{ status: "completed" }],
        );
        yield* Scope.close(firstScope, Exit.void);

        const failedScope = yield* Scope.make("sequential");
        const failed = yield* buildRuntime(filename, failedScope, defaultHooks, Stream.never, () =>
          Effect.die(new Error("injected publication crash")),
        );
        yield* seedTask(failed, "publication-restart-dead");
        const deadSource = seedCommittedVerificationFinalization(
          filename,
          "publication-restart-dead",
          "passed",
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(failed.finalizer.processHandoff(deadSource.handoffId))),
        );
        assert.equal(yield* Ref.get(failed.publications), 0);
        assert.deepStrictEqual(
          yield* failed.sql<{ readonly status: string }>`
            SELECT status FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${deadSource.handoffId}
          `,
          [{ status: "claimed" }],
        );
        yield* Scope.close(failedScope, Exit.void);
        yield* TestClock.adjust(Duration.minutes(1));

        const restartScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(restartScope, Exit.void));
        const restart = yield* buildRuntime(filename, restartScope, {
          ...defaultHooks,
          recoveryPageSize: 1,
        });
        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 1);
        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 1);
        assert.equal((yield* restart.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.equal(yield* Ref.get(restart.publications), 1);
        assert.equal(
          (yield* restart.finalizer.processHandoff(deadSource.handoffId))._tag,
          "Replayed",
        );
        assert.equal(yield* Ref.get(restart.publications), 1);

        yield* seedTask(restart, "publication-restart-later");
        seedCommittedVerificationFinalization(filename, "publication-restart-later", "passed");
        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 2);
        assert.deepStrictEqual(yield* finalizationCounts(restart.sql), [
          { evidence: 3, events: 3, markers: 3, publications: 3, receipts: 3 },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "deduplicates one EventId after physical publication fails before durable completion",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-after-physical-publication-",
        });
        const filename = `${directory}/state.sqlite`;
        const setupScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
        const setup = yield* buildRuntime(filename, setupScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        yield* seedTask(setup, "after-physical-publication");
        const source = seedCommittedVerificationFinalization(
          filename,
          "after-physical-publication",
          "passed",
        );

        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const productionEngine = yield* buildProductionTaskEngine(filename, engineScope);
        const observed = yield* Ref.make<ReadonlyArray<string>>([]);
        const firstObserved = yield* Deferred.make<void>();
        const sentinelObserved = yield* Deferred.make<void>();
        const subscribed = yield* productionEngine.subscribeDomainEvents;
        const listener = yield* Effect.forkScoped(
          Stream.runForEach(subscribed, (event) =>
            Ref.update(observed, (current) => [...current, event.eventId]).pipe(
              Effect.andThen(
                event.eventId === "task-finalization-publication-sentinel"
                  ? Deferred.succeed(sentinelObserved, undefined)
                  : Deferred.succeed(firstObserved, undefined),
              ),
              Effect.asVoid,
            ),
          ),
        );

        let failAfterPhysicalPublication = true;
        const failure = new Error("injected failure after physical task publication");
        const finalizerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(finalizerScope, Exit.void));
        const runtime = yield* buildRuntime(
          filename,
          finalizerScope,
          {
            ...defaultHooks,
            afterPublicationBeforeCompletion: () => {
              if (!failAfterPhysicalPublication) return Effect.void;
              failAfterPhysicalPublication = false;
              return Deferred.await(firstObserved).pipe(Effect.andThen(Effect.die(failure)));
            },
          },
          Stream.never,
          () => Effect.void,
          productionEngine,
        );

        const first = yield* Effect.exit(runtime.finalizer.processHandoff(source.handoffId));
        assert.isTrue(Exit.isFailure(first));
        if (Exit.isFailure(first)) {
          assert.isTrue(Cause.hasDies(first.cause));
          assert.include(Cause.pretty(first.cause), failure.message);
        }
        const committedEvent = yield* runtime.sql<{ readonly eventId: string }>`
          SELECT task_event_id AS "eventId"
          FROM main.agent_control_task_verification_finalization_markers
          WHERE handoff_id = ${source.handoffId}
        `;
        assert.deepStrictEqual(yield* Ref.get(observed), [committedEvent[0]!.eventId]);
        assert.deepStrictEqual(
          yield* runtime.sql<{ readonly status: string; readonly fence: number }>`
            SELECT status, claim_fence AS fence
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          [{ fence: 1, status: "claimed" }],
        );

        assert.equal((yield* runtime.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.deepStrictEqual(
          yield* runtime.sql<{ readonly status: string; readonly fence: number }>`
            SELECT status, claim_fence AS fence
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          [{ fence: 1, status: "completed" }],
        );
        const terminalEvents = yield* runtime.events.readStream(source.taskId, 1, 1);
        assert.lengthOf(terminalEvents, 1);
        yield* productionEngine.publishCommitted([
          {
            ...terminalEvents[0]!,
            eventId: EventId.make("task-finalization-publication-sentinel"),
          },
        ]);
        yield* Deferred.await(sentinelObserved);
        yield* Fiber.interrupt(listener);
        assert.deepStrictEqual(yield* Ref.get(observed), [
          committedEvent[0]!.eventId,
          "task-finalization-publication-sentinel",
        ]);
        yield* runtime.finalizer.recover;
        assert.lengthOf(yield* Ref.get(observed), 2);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect(
  "binds one authoritative Clock across production layers while a caller override cannot steal a live publication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const startTime = Date.parse("2026-08-30T14:00:00.000Z");
        yield* TestClock.setTime(startTime);
        const authoritativeClock = yield* Clock.Clock;
        const aheadCallerTime = startTime + Duration.toMillis(Duration.days(1));
        const aheadCallerClock: Clock.Clock = {
          currentTimeMillisUnsafe: () => aheadCallerTime,
          currentTimeMillis: Effect.succeed(aheadCallerTime),
          currentTimeNanosUnsafe: () => BigInt(aheadCallerTime) * 1_000_000n,
          currentTimeNanos: Effect.succeed(BigInt(aheadCallerTime) * 1_000_000n),
          sleep: (duration) => authoritativeClock.sleep(duration),
        };
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-shared-runtime-publication-",
        });
        const filename = `${directory}/state.sqlite`;
        const setupScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
        const setup = yield* buildRuntime(filename, setupScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        yield* seedTask(setup, "shared-runtime-publication");
        const source = seedCommittedVerificationFinalization(
          filename,
          "shared-runtime-publication",
          "passed",
        );

        const firstEngineScope = yield* Scope.make("sequential");
        const secondEngineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(secondEngineScope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(firstEngineScope, Exit.void));
        const firstEngine = yield* buildProductionTaskEngine(filename, firstEngineScope);
        const secondEngine = yield* buildProductionTaskEngine(filename, secondEngineScope);
        assert.notStrictEqual(firstEngine, secondEngine);

        const firstObserved = yield* Ref.make<ReadonlyArray<string>>([]);
        const secondObserved = yield* Ref.make<ReadonlyArray<string>>([]);
        const firstPhysicalPublication = yield* Deferred.make<void>();
        const firstEvents = yield* firstEngine.subscribeDomainEvents;
        const secondEvents = yield* secondEngine.subscribeDomainEvents;
        yield* Effect.forkScoped(
          Stream.runForEach(firstEvents, (event) =>
            Ref.update(firstObserved, (current) => [...current, event.eventId]).pipe(
              Effect.andThen(Deferred.succeed(firstPhysicalPublication, undefined)),
              Effect.asVoid,
            ),
          ),
        );
        yield* Effect.forkScoped(
          Stream.runForEach(secondEvents, (event) =>
            Ref.update(secondObserved, (current) => [...current, event.eventId]),
          ),
        );

        const afterPhysicalPublication = yield* Deferred.make<void>();
        const crashFirstRuntime = yield* Deferred.make<void>();
        const firstFinalizerScope = yield* Scope.make("sequential");
        const secondFinalizerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(secondFinalizerScope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(firstFinalizerScope, Exit.void));
        const first = yield* buildRuntime(
          filename,
          firstFinalizerScope,
          {
            ...defaultHooks,
            publicationOwnerId: "11111111-1111-4111-8111-111111111111",
            afterPublicationBeforeCompletion: () =>
              Deferred.succeed(afterPhysicalPublication, undefined).pipe(
                Effect.andThen(Deferred.await(crashFirstRuntime)),
                Effect.andThen(Effect.die(new Error("simulated publication runtime death"))),
              ),
          },
          Stream.never,
          () => Effect.void,
          firstEngine,
        );
        const second = yield* buildRuntime(
          filename,
          secondFinalizerScope,
          {
            ...defaultHooks,
            publicationOwnerId: "22222222-2222-4222-8222-222222222222",
          },
          Stream.never,
          () => Effect.void,
          secondEngine,
        );

        const firstFiber = yield* Effect.forkScoped(
          first.finalizer.processHandoff(source.handoffId),
        );
        yield* Deferred.await(afterPhysicalPublication);
        yield* Deferred.await(firstPhysicalPublication);
        assert.lengthOf(yield* Ref.get(firstObserved), 1);
        assert.lengthOf(yield* Ref.get(secondObserved), 0);

        // Advancing the one runtime Clock beyond several original lease windows drives the real
        // heartbeat. A second production layer still observes a live authoritative claim.
        yield* TestClock.adjust(Duration.minutes(1));
        const liveBeforeCallerOverride = (yield* second.sql<{
          readonly expiresAt: string;
          readonly fence: number;
          readonly owner: string;
          readonly revision: number;
          readonly status: string;
        }>`
          SELECT publication_owner_id AS owner, claim_fence AS fence, revision,
            lease_expires_at AS "expiresAt", status
          FROM main.agent_control_task_verification_finalization_publications
          WHERE handoff_id = ${source.handoffId}
        `)[0]!;
        assert.equal(
          (yield* second.finalizer
            .processHandoff(source.handoffId)
            .pipe(Effect.provideService(Clock.Clock, aheadCallerClock)))._tag,
          "Replayed",
        );
        assert.lengthOf(yield* Ref.get(firstObserved), 1);
        assert.lengthOf(yield* Ref.get(secondObserved), 0);
        const liveAfterCallerOverride = (yield* second.sql<{
          readonly expiresAt: string;
          readonly fence: number;
          readonly owner: string;
          readonly revision: number;
          readonly status: string;
        }>`
          SELECT publication_owner_id AS owner, claim_fence AS fence, revision,
            lease_expires_at AS "expiresAt", status
          FROM main.agent_control_task_verification_finalization_publications
          WHERE handoff_id = ${source.handoffId}
        `)[0]!;
        assert.deepStrictEqual(liveAfterCallerOverride, liveBeforeCallerOverride);
        assert.equal(liveAfterCallerOverride.owner, "11111111-1111-4111-8111-111111111111");
        assert.equal(liveAfterCallerOverride.fence, 1);
        assert.equal(liveAfterCallerOverride.status, "claimed");
        assert.isAbove(Date.parse(liveAfterCallerOverride.expiresAt), startTime + 60_000);

        yield* Deferred.succeed(crashFirstRuntime, undefined);
        const firstExit = yield* Fiber.await(firstFiber);
        assert.isTrue(Exit.isFailure(firstExit));
        yield* Scope.close(firstFinalizerScope, Exit.void);
        yield* Scope.close(firstEngineScope, Exit.void);

        yield* TestClock.adjust(
          Duration.millis(Date.parse(liveAfterCallerOverride.expiresAt) - (startTime + 60_000)),
        );
        yield* second.finalizer.recover;
        assert.lengthOf(yield* Ref.get(firstObserved), 1);
        assert.lengthOf(yield* Ref.get(secondObserved), 0);
        assert.equal(
          (yield* Ref.get(firstObserved)).length + (yield* Ref.get(secondObserved)).length,
          1,
        );
        assert.deepStrictEqual(
          yield* second.sql<{
            readonly fence: number;
            readonly owner: string;
            readonly status: string;
          }>`
            SELECT publication_owner_id AS owner, claim_fence AS fence, status
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          [
            {
              fence: 2,
              owner: "22222222-2222-4222-8222-222222222222",
              status: "completed",
            },
          ],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect(
  "fences two independent WAL finalizers until lease expiry and rejects the stale owner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-fenced-claim-",
        });
        const filename = `${directory}/state.sqlite`;
        const setupScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
        const setup = yield* buildRuntime(filename, setupScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        yield* seedTask(setup, "fenced-claim");
        const source = seedCommittedVerificationFinalization(filename, "fenced-claim", "passed");

        const firstClaimed = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        let blockFirstFence = true;
        const firstClock = Date.parse("2026-08-30T12:00:00.000Z");
        yield* TestClock.setTime(firstClock);
        const leftScope = yield* Scope.make("sequential");
        const rightScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(leftScope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(rightScope, Exit.void));
        const left = yield* buildRuntime(filename, leftScope, {
          ...defaultHooks,
          beforePublicationFenceValidation: () =>
            Effect.suspend(() => {
              if (!blockFirstFence) return Effect.void;
              blockFirstFence = false;
              return Deferred.succeed(firstClaimed, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirst)),
              );
            }),
        });
        const right = yield* buildRuntime(filename, rightScope, defaultHooks);

        const leftFiber = yield* Effect.forkScoped(left.finalizer.processHandoff(source.handoffId));
        yield* Deferred.await(firstClaimed);
        const liveClaim = yield* setup.sql<{
          readonly owner: string;
          readonly fence: number;
          readonly revision: number;
          readonly expiresAt: string;
        }>`
          SELECT publication_owner_id AS owner, claim_fence AS fence, revision,
            lease_expires_at AS "expiresAt"
          FROM main.agent_control_task_verification_finalization_publications
          WHERE handoff_id = ${source.handoffId}
        `;
        assert.equal(liveClaim[0]?.fence, 1);
        assert.equal((yield* right.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.equal(yield* Ref.get(right.publications), 0);
        assert.deepStrictEqual(
          yield* setup.sql<{
            readonly owner: string;
            readonly fence: number;
            readonly revision: number;
            readonly expiresAt: string;
          }>`
            SELECT publication_owner_id AS owner, claim_fence AS fence, revision,
              lease_expires_at AS "expiresAt"
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          liveClaim,
        );

        yield* TestClock.adjust(Duration.minutes(1));
        assert.equal((yield* right.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.equal(yield* Ref.get(right.publications), 1);
        yield* Deferred.succeed(releaseFirst, undefined);
        const leftExit = yield* Fiber.await(leftFiber);
        assert.isTrue(Exit.isSuccess(leftExit));
        assert.equal(yield* Ref.get(left.publications), 0);
        assert.deepStrictEqual(
          yield* setup.sql<{
            readonly status: string;
            readonly fence: number;
            readonly revision: number;
          }>`
            SELECT status, claim_fence AS fence, revision
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `,
          [{ fence: 2, revision: 5, status: "completed" }],
        );
        assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
          { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect(
  "wakes each foreign publication claim once at its exact deadline without losing later keyset candidates",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-publication-deadlines-",
        });
        const filename = `${directory}/state.sqlite`;
        const firstClock = Date.parse("2026-08-30T12:00:00.000Z");
        yield* TestClock.setTime(firstClock);
        const seedScope = yield* Scope.make("sequential");
        const seeded = yield* buildRuntime(filename, seedScope, defaultHooks, Stream.never, () =>
          Effect.die(new Error("leave foreign publication claim live")),
        );
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, seeded.sql),
        );

        yield* seedTask(seeded, "deadline-a");
        const firstSource = seedCommittedVerificationFinalization(filename, "deadline-a", "passed");
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(seeded.finalizer.processHandoff(firstSource.handoffId)),
          ),
        );
        yield* TestClock.adjust(Duration.seconds(5));
        yield* seedTask(seeded, "deadline-b");
        const secondSource = seedCommittedVerificationFinalization(
          filename,
          "deadline-b",
          "passed",
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(seeded.finalizer.processHandoff(secondSource.handoffId)),
          ),
        );
        yield* seedTask(seeded, "deadline-z-later");
        const laterSource = seedCommittedVerificationFinalization(
          filename,
          "deadline-z-later",
          "passed",
        );
        const claimed = yield* seeded.sql<{
          readonly handoffId: string;
          readonly expiresAt: string;
          readonly fence: number;
          readonly revision: number;
          readonly status: string;
        }>`
          SELECT handoff_id AS "handoffId", lease_expires_at AS "expiresAt",
            claim_fence AS fence, revision, status
          FROM main.agent_control_task_verification_finalization_publications
          ORDER BY handoff_id
        `;
        assert.deepStrictEqual(
          claimed.map(({ handoffId, fence, revision, status }) => ({
            handoffId,
            fence,
            revision,
            status,
          })),
          [
            {
              fence: 1,
              handoffId: firstSource.handoffId,
              revision: 3,
              status: "claimed",
            },
            {
              fence: 1,
              handoffId: secondSource.handoffId,
              revision: 3,
              status: "claimed",
            },
          ],
        );
        const firstDeadline = Date.parse(claimed[0]!.expiresAt);
        const secondDeadline = Date.parse(claimed[1]!.expiresAt);
        assert.equal(firstDeadline, firstClock + 30_000);
        assert.equal(secondDeadline, firstClock + 35_000);
        yield* Scope.close(seedScope, Exit.void);

        yield* TestClock.adjust(Duration.seconds(5));
        const recoveryScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(recoveryScope, Exit.void));
        const restart = yield* buildRuntime(filename, recoveryScope, {
          ...defaultHooks,
          recoveryPageSize: 1,
        });
        const ownerScope = yield* Scope.make("sequential");
        yield* restart.finalizer.prepare(Effect.void).pipe(Scope.provide(ownerScope));
        yield* restart.finalizer.drain;

        assert.equal(yield* Ref.get(restart.publications), 1);
        assert.equal(
          Option.getOrThrow(yield* restart.states.get(laterSource.taskId)).stage,
          "verification",
        );
        assert.deepStrictEqual(
          yield* restart.sql<{ readonly fence: number; readonly revision: number }>`
            SELECT claim_fence AS fence, revision
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id IN (${firstSource.handoffId}, ${secondSource.handoffId})
            ORDER BY handoff_id
          `,
          [
            { fence: 1, revision: 3 },
            { fence: 1, revision: 3 },
          ],
        );

        // Repeated recovery replaces neither one-shot timer and must not enqueue early.
        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 1);

        yield* TestClock.adjust(Duration.seconds(19));
        yield* restart.finalizer.drain;
        assert.equal(yield* Ref.get(restart.publications), 1);
        yield* TestClock.adjust(Duration.seconds(1));
        yield* restart.finalizer.drain;
        assert.equal(yield* Ref.get(restart.publications), 2);
        assert.deepStrictEqual(
          yield* restart.sql<{ readonly fence: number; readonly status: string }>`
            SELECT claim_fence AS fence, status
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${firstSource.handoffId}
          `,
          [{ fence: 2, status: "completed" }],
        );

        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 2);
        yield* Scope.close(ownerScope, Exit.void);
        yield* TestClock.adjust(Duration.seconds(5));
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(restart.publications), 2);
        assert.deepStrictEqual(
          yield* restart.sql<{
            readonly fence: number;
            readonly revision: number;
            readonly status: string;
          }>`
            SELECT claim_fence AS fence, revision, status
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${secondSource.handoffId}
          `,
          [{ fence: 1, revision: 3, status: "claimed" }],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "enforces the installed same-fence renewal deadline and requires fence plus one for takeover",
  () =>
    withDatabase(
      "task-verification-finalizer-installed-renewal-guard-",
      (filename, runtime) =>
        Effect.gen(function* () {
          yield* seedTask(runtime, "installed-renewal-guard");
          const source = seedCommittedVerificationFinalization(
            filename,
            "installed-renewal-guard",
            "passed",
          );
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(runtime.finalizer.processHandoff(source.handoffId))),
          );

          const database = new NodeSqlite.DatabaseSync(filename);
          try {
            database.exec(
              "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 0",
            );
            const initial = database
              .prepare(`UPDATE main.agent_control_task_verification_finalization_publications
                SET publication_owner_id = ?, status = 'claimed', revision = revision + 1,
                  claim_fence = 1, claimed_at = ?, lease_expires_at = ?
                WHERE handoff_id = ? AND status = 'pending' AND revision = 1`)
              .run(
                "11111111-1111-4111-8111-111111111111",
                "2026-08-30T13:00:00.000Z",
                "2026-08-30T13:00:30.000Z",
                source.handoffId,
              );
            assert.equal(initial.changes, 1);

            const renew = database.prepare(
              `UPDATE main.agent_control_task_verification_finalization_publications
               SET revision = revision + 1, claimed_at = ?, lease_expires_at = ?
               WHERE handoff_id = ?`,
            );
            assert.equal(
              renew.run("2026-08-30T13:00:29.000Z", "2026-08-30T13:00:59.000Z", source.handoffId)
                .changes,
              1,
            );
            assert.throws(
              () =>
                renew.run("2026-08-30T13:00:59.000Z", "2026-08-30T13:01:29.000Z", source.handoffId),
              /invalid task Verification publication transition/,
            );
            assert.throws(
              () =>
                renew.run("2026-08-30T13:01:00.000Z", "2026-08-30T13:01:30.000Z", source.handoffId),
              /invalid task Verification publication transition/,
            );

            const takeover = database.prepare(
              `UPDATE main.agent_control_task_verification_finalization_publications
               SET publication_owner_id = ?, revision = revision + 1, claim_fence = ?,
                 claimed_at = ?, lease_expires_at = ?
               WHERE handoff_id = ?`,
            );
            assert.throws(
              () =>
                takeover.run(
                  "22222222-2222-4222-8222-222222222222",
                  1,
                  "2026-08-30T13:01:00.000Z",
                  "2026-08-30T13:01:30.000Z",
                  source.handoffId,
                ),
              /invalid task Verification publication transition/,
            );
            assert.equal(
              takeover.run(
                "22222222-2222-4222-8222-222222222222",
                2,
                "2026-08-30T13:01:00.000Z",
                "2026-08-30T13:01:30.000Z",
                source.handoffId,
              ).changes,
              1,
            );
            assert.deepStrictEqual(
              database
                .prepare(`SELECT publication_owner_id AS owner, claim_fence AS fence, revision
                  FROM main.agent_control_task_verification_finalization_publications
                  WHERE handoff_id = ?`)
                .get(source.handoffId),
              {
                fence: 2,
                owner: "22222222-2222-4222-8222-222222222222",
                revision: 4,
              },
            );
          } finally {
            database.close();
          }
        }),
      {
        ...defaultHooks,
        beforePublicationClaim: () => Effect.die(new Error("leave publication pending")),
      },
    ),
  30_000,
);

it.effect(
  "reacquires an expired same-owner claim with a new fence before the stale continuation resumes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-same-owner-fence-",
        });
        const filename = `${directory}/state.sqlite`;
        const startTime = Date.parse("2026-08-30T13:00:00.000Z");
        yield* TestClock.setTime(startTime);
        const firstFenceReached = yield* Deferred.make<void>();
        const releaseStaleContinuation = yield* Deferred.make<void>();
        let blockFirstFence = true;
        const setupScope = yield* Scope.make("sequential");
        const setup = yield* buildRuntime(filename, setupScope, {
          ...defaultHooks,
          beforePublicationClaim: () => Effect.die(new Error("leave publication pending")),
        });
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        yield* seedTask(setup, "same-owner-fence");
        const source = seedCommittedVerificationFinalization(
          filename,
          "same-owner-fence",
          "passed",
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(setup.finalizer.processHandoff(source.handoffId))),
        );
        yield* Scope.close(setupScope, Exit.void);

        const claimDatabase = new NodeSqlite.DatabaseSync(filename);
        claimDatabase.exec(
          "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 0",
        );
        claimDatabase
          .prepare(`UPDATE main.agent_control_task_verification_finalization_publications
            SET publication_owner_id = ?, status = 'claimed', revision = 2, claim_fence = 1,
              claimed_at = ?, lease_expires_at = ?
            WHERE handoff_id = ? AND status = 'pending' AND revision = 1`)
          .run(
            "11111111-1111-4111-8111-111111111111",
            "2026-08-30T13:00:00.000Z",
            "2026-08-30T13:00:30.000Z",
            source.handoffId,
          );
        claimDatabase.close();

        const staleScope = yield* Scope.make("sequential");
        const takeoverScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(staleScope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(takeoverScope, Exit.void));
        const stale = yield* buildRuntime(filename, staleScope, {
          ...defaultHooks,
          publicationOwnerId: "11111111-1111-4111-8111-111111111111",
          beforePublicationFenceValidation: () =>
            Effect.suspend(() => {
              if (!blockFirstFence) return Effect.void;
              blockFirstFence = false;
              return Deferred.succeed(firstFenceReached, undefined).pipe(
                Effect.andThen(Deferred.await(releaseStaleContinuation)),
              );
            }),
        });
        const staleFiber = yield* Effect.forkScoped(
          stale.finalizer.processHandoff(source.handoffId),
        );
        yield* Deferred.await(firstFenceReached);

        const takeover = yield* buildRuntime(filename, takeoverScope, {
          ...defaultHooks,
          publicationOwnerId: "11111111-1111-4111-8111-111111111111",
        });
        const oldClaim = (yield* takeover.sql<{
          readonly expiresAt: string;
          readonly fence: number;
          readonly owner: string;
          readonly revision: number;
        }>`
            SELECT publication_owner_id AS owner, claim_fence AS fence, revision,
              lease_expires_at AS "expiresAt"
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `)[0]!;
        assert.equal(oldClaim.fence, 1);
        assert.equal(oldClaim.revision, 2);

        yield* TestClock.adjust(Duration.millis(Date.parse(oldClaim.expiresAt) - startTime));
        assert.equal((yield* takeover.finalizer.processHandoff(source.handoffId))._tag, "Replayed");
        assert.equal(yield* Ref.get(takeover.publications), 1);
        const completed = (yield* takeover.sql<{
          readonly fence: number;
          readonly owner: string;
          readonly revision: number;
          readonly status: string;
        }>`
            SELECT publication_owner_id AS owner, claim_fence AS fence, revision, status
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = ${source.handoffId}
          `)[0]!;
        assert.equal(completed.owner, oldClaim.owner);
        assert.deepStrictEqual(
          { fence: completed.fence, revision: completed.revision, status: completed.status },
          { fence: 2, revision: 5, status: "completed" },
        );

        yield* Deferred.succeed(releaseStaleContinuation, undefined);
        const staleExit = yield* Fiber.await(staleFiber);
        assert.isTrue(Exit.isSuccess(staleExit));
        assert.equal(yield* Ref.get(stale.publications), 0);
        assert.equal(yield* Ref.get(takeover.publications), 1);
        assert.lengthOf(yield* Ref.get(stale.publishedEvents), 0);
        assert.lengthOf(yield* Ref.get(takeover.publishedEvents), 1);
        assert.deepStrictEqual(yield* finalizationCounts(takeover.sql), [
          { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "retries one WAL claim failure and one post-publication completion failure without duplicates",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-publication-wal-retry-",
        });
        const filename = `${directory}/state.sqlite`;
        const afterClaimCommit = yield* Deferred.make<void>();
        const releaseClaim = yield* Deferred.make<void>();
        const afterPhysicalPublication = yield* Deferred.make<void>();
        const releaseCompletion = yield* Deferred.make<void>();
        const failedOperations: Array<string> = [];
        let claimBarrierUsed = false;
        let completionBarrierUsed = false;
        let claimLock = false;
        let completionLock = false;
        const database = new NodeSqlite.DatabaseSync(filename);
        yield* Effect.addFinalizer(() => Effect.sync(() => database.close()));

        const runtimeScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
        const runtime = yield* buildRuntime(filename, runtimeScope, {
          ...defaultHooks,
          afterCommit: (handoffId) =>
            handoffId === "handoff-wal-claim-retry" && !claimBarrierUsed
              ? Effect.sync(() => {
                  claimBarrierUsed = true;
                }).pipe(
                  Effect.andThen(Deferred.succeed(afterClaimCommit, undefined)),
                  Effect.andThen(Deferred.await(releaseClaim)),
                )
              : Effect.void,
          afterPublicationBeforeCompletion: (handoffId) =>
            handoffId === "handoff-wal-completion-retry" && !completionBarrierUsed
              ? Effect.sync(() => {
                  completionBarrierUsed = true;
                }).pipe(
                  Effect.andThen(Deferred.succeed(afterPhysicalPublication, undefined)),
                  Effect.andThen(Deferred.await(releaseCompletion)),
                )
              : Effect.void,
          afterPublicationAttemptFailure: (_handoffId, operation) =>
            Effect.sync(() => {
              failedOperations.push(operation);
              if (operation === "publication-claim" && claimLock) {
                database.exec("ROLLBACK");
                claimLock = false;
              }
              if (operation === "publication-complete" && completionLock) {
                database.exec("ROLLBACK");
                completionLock = false;
              }
            }),
        });
        yield* runtime.sql`PRAGMA busy_timeout = 0`;
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, runtime.sql),
        );
        database.exec(
          "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 0",
        );

        yield* seedTask(runtime, "wal-claim-retry");
        const claimSource = seedCommittedVerificationFinalization(
          filename,
          "wal-claim-retry",
          "passed",
        );
        const claimFiber = yield* Effect.forkScoped(
          runtime.finalizer.processHandoff(claimSource.handoffId),
        );
        yield* Deferred.await(afterClaimCommit);
        database.exec("BEGIN IMMEDIATE");
        claimLock = true;
        yield* Deferred.succeed(releaseClaim, undefined);
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(claimFiber)));
        assert.equal(claimLock, false);
        assert.deepStrictEqual(failedOperations, ["publication-claim"]);
        assert.equal(yield* Ref.get(runtime.publications), 1);

        yield* seedTask(runtime, "wal-completion-retry");
        const completionSource = seedCommittedVerificationFinalization(
          filename,
          "wal-completion-retry",
          "passed",
        );
        const completionFiber = yield* Effect.forkScoped(
          runtime.finalizer.processHandoff(completionSource.handoffId),
        );
        yield* Deferred.await(afterPhysicalPublication);
        database.exec("BEGIN IMMEDIATE");
        completionLock = true;
        yield* Deferred.succeed(releaseCompletion, undefined);
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(completionFiber)));
        assert.equal(completionLock, false);
        assert.deepStrictEqual(failedOperations, ["publication-claim", "publication-complete"]);
        assert.equal(yield* Ref.get(runtime.publications), 2);

        assert.equal(
          (yield* runtime.finalizer.processHandoff(claimSource.handoffId))._tag,
          "Replayed",
        );
        assert.equal(
          (yield* runtime.finalizer.processHandoff(completionSource.handoffId))._tag,
          "Replayed",
        );
        yield* runtime.finalizer.recover;
        assert.equal(yield* Ref.get(runtime.publications), 2);

        yield* seedTask(runtime, "wal-publication-later");
        const later = seedCommittedVerificationFinalization(
          filename,
          "wal-publication-later",
          "passed",
        );
        assert.equal((yield* runtime.finalizer.processHandoff(later.handoffId))._tag, "Finalized");
        assert.equal(yield* Ref.get(runtime.publications), 3);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "bounds recovery retries, keeps the worker alive, and drains a later publication candidate",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-bounded-publication-recovery-",
        });
        const filename = `${directory}/state.sqlite`;
        const seedScope = yield* Scope.make("sequential");
        const seeded = yield* buildRuntime(filename, seedScope, {
          ...defaultHooks,
          beforePublicationClaim: () =>
            Effect.die(new Error("leave committed marker publication pending")),
        });
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, seeded.sql),
        );
        for (const suffix of ["bounded-recovery-a", "bounded-recovery-b"] as const) {
          yield* seedTask(seeded, suffix);
          const source = seedCommittedVerificationFinalization(filename, suffix, "passed");
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(seeded.finalizer.processHandoff(source.handoffId))),
          );
        }
        assert.equal(yield* Ref.get(seeded.publications), 0);
        yield* Scope.close(seedScope, Exit.void);

        const restartScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(restartScope, Exit.void));
        const restart = yield* buildRuntime(filename, restartScope, {
          ...defaultHooks,
          recoveryPageSize: 1,
        });
        yield* restart.sql.unsafe(`
          CREATE TEMP TRIGGER fail_bounded_recovery_a_completion
          BEFORE UPDATE OF status
          ON main.agent_control_task_verification_finalization_publications
          WHEN OLD.handoff_id = 'handoff-bounded-recovery-a' AND NEW.status = 'completed'
          BEGIN SELECT RAISE(ABORT, 'injected bounded completion failure'); END
        `).unprepared;
        const directFailure = yield* Effect.result(
          restart.finalizer.processHandoff("handoff-bounded-recovery-a"),
        );
        assert.equal(directFailure._tag, "Failure");
        if (directFailure._tag === "Failure") {
          assert.equal(directFailure.failure.operation, "publication-complete");
          assert.equal(directFailure.failure.reason, "persistence");
        }
        assert.equal(yield* Ref.get(restart.publications), 1);
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        yield* restart.finalizer.prepare(Effect.void).pipe(Scope.provide(ownerScope));
        yield* restart.finalizer.drain;

        assert.equal(yield* Ref.get(restart.publications), 2);
        assert.deepStrictEqual(
          yield* restart.sql<{
            readonly handoffId: string;
            readonly status: string;
            readonly revision: number;
            readonly fence: number;
          }>`
            SELECT handoff_id AS "handoffId", status, revision, claim_fence AS fence
            FROM main.agent_control_task_verification_finalization_publications
            ORDER BY handoff_id
          `,
          [
            {
              fence: 1,
              handoffId: "handoff-bounded-recovery-a",
              revision: 6,
              status: "claimed",
            },
            {
              fence: 1,
              handoffId: "handoff-bounded-recovery-b",
              revision: 4,
              status: "completed",
            },
          ],
        );

        yield* restart.sql.unsafe(`DROP TRIGGER temp.fail_bounded_recovery_a_completion`)
          .unprepared;
        yield* restart.finalizer.recover;
        assert.equal(yield* Ref.get(restart.publications), 2);
        assert.deepStrictEqual(
          yield* restart.sql<{ readonly status: string; readonly revision: number }>`
            SELECT status, revision
            FROM main.agent_control_task_verification_finalization_publications
            WHERE handoff_id = 'handoff-bounded-recovery-a'
          `,
          [{ revision: 8, status: "completed" }],
        );
        yield* restart.finalizer.recover;
        assert.equal(
          (yield* restart.finalizer.processHandoff("handoff-bounded-recovery-a"))._tag,
          "Replayed",
        );
        assert.equal(yield* Ref.get(restart.publications), 2);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live("loses a deterministic WAL source-gate race without stale overwrite or publication", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "task-verification-finalizer-race-",
      });
      const filename = `${directory}/state.sqlite`;
      const setupScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
      const setup = yield* buildRuntime(filename, setupScope);
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, setup.sql),
      );
      const raceCreated = makeCreatedDraft("race");
      yield* seedTask(setup, "race");
      const source = seedCommittedVerificationFinalization(filename, "race", "passed");

      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const loserScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(loserScope, Exit.void));
      const loser = yield* buildRuntime(filename, loserScope, {
        ...defaultHooks,
        afterAuthoritativeRead: () =>
          Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
      });
      const writerScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(writerScope, Exit.void));
      const writer = yield* buildRuntime(filename, writerScope);
      const loserFiber = yield* Effect.forkScoped(loser.finalizer.processHandoff(source.handoffId));
      yield* Deferred.await(reached);

      const changedDraft: AgentControlTaskEventDraft = {
        eventId: EventId.make("task-source-gate-race"),
        type: "agentControl.task.sourceGate.changed",
        aggregateKind: "task",
        aggregateId: source.taskId,
        occurredAt: sourceChangedAt,
        commandId: CommandId.make("task-source-gate-race-command"),
        causationEventId: null,
        correlationId: CommandId.make("task-source-gate-race-command"),
        authority: "controller",
        metadata: { schemaVersion: 1 },
        payload: {
          taskId: source.taskId,
          source: raceCreated.payload.source,
          previousSourceGate: "eligible",
          sourceGate: "not-ready",
          sourceUpdatedAt: sourceChangedAt,
          githubIntakeSequence: 2,
          sourceSnapshot: {
            ...raceCreated.payload.sourceSnapshot,
            updatedAt: sourceChangedAt,
            ready: false,
            eligible: false,
            eligibilityReason: "ready-inactive",
          },
          changedAt: sourceChangedAt,
        },
      };
      const changed = yield* writer.events.append({
        taskId: source.taskId,
        expectedStreamVersion: 1,
        events: [changedDraft],
      });
      yield* writer.projection.projectEvent(changed[0]!);
      yield* Deferred.succeed(release, undefined);
      const loserResult = yield* Fiber.await(loserFiber);
      assert.isTrue(Exit.isFailure(loserResult));
      assert.equal(yield* Ref.get(loser.publications), 0);
      assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
        { evidence: 0, events: 0, markers: 0, publications: 0, receipts: 0 },
      ]);
      const afterRace = Option.getOrThrow(yield* setup.states.get(source.taskId));
      assert.equal(afterRace.revision, 2);
      assert.equal(afterRace.sourceGate, "not-ready");
      assert.equal(afterRace.stage, "intake");

      assert.equal((yield* setup.finalizer.processHandoff(source.handoffId))._tag, "Finalized");
      const terminal = Option.getOrThrow(yield* setup.states.get(source.taskId));
      assert.equal(terminal.revision, 3);
      assert.equal(terminal.sourceGate, "not-ready");
      assert.equal(terminal.stage, "verification");
      assert.equal(yield* Ref.get(setup.publications), 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("publishes one concurrent WAL winner and keeps the independent loser at zero", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "task-verification-finalizer-concurrent-winner-",
      });
      const filename = `${directory}/state.sqlite`;
      const setupScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
      const setup = yield* buildRuntime(filename, setupScope);
      yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, setup.sql),
      );
      yield* seedTask(setup, "concurrent-winner");
      const source = seedCommittedVerificationFinalization(filename, "concurrent-winner", "passed");

      const readers = yield* Ref.make(0);
      const bothRead = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const afterRead = () =>
        Ref.updateAndGet(readers, (count) => count + 1).pipe(
          Effect.flatMap((count) =>
            count === 2 ? Deferred.succeed(bothRead, undefined) : Effect.void,
          ),
          Effect.andThen(Deferred.await(release)),
        );
      const leftScope = yield* Scope.make("sequential");
      const rightScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(leftScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(rightScope, Exit.void));
      const left = yield* buildRuntime(filename, leftScope, {
        ...defaultHooks,
        afterAuthoritativeRead: afterRead,
      });
      const right = yield* buildRuntime(filename, rightScope, {
        ...defaultHooks,
        afterAuthoritativeRead: afterRead,
      });
      const leftFiber = yield* Effect.forkScoped(left.finalizer.processHandoff(source.handoffId));
      const rightFiber = yield* Effect.forkScoped(right.finalizer.processHandoff(source.handoffId));
      yield* Deferred.await(bothRead);
      yield* Deferred.succeed(release, undefined);
      const [leftExit, rightExit] = yield* Effect.all([
        Fiber.await(leftFiber),
        Fiber.await(rightFiber),
      ]);
      assert.isTrue(Exit.isSuccess(leftExit) || Exit.isSuccess(rightExit));
      const counts = [yield* Ref.get(left.publications), yield* Ref.get(right.publications)].sort();
      assert.deepStrictEqual(counts, [0, 1]);
      assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
        { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
      ]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "keeps the prepared production worker alive across a WAL race retry and later candidates",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-worker-race-",
        });
        const filename = `${directory}/state.sqlite`;
        const setupScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(setupScope, Exit.void));
        const setup = yield* buildRuntime(filename, setupScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        const raceDraft = makeCreatedDraft("worker-race");
        yield* seedTask(setup, "worker-race");
        const raceSource = seedCommittedVerificationFinalization(filename, "worker-race", "passed");

        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const laterAttempted = yield* Deferred.make<void>();
        const leaseEvents = yield* PubSub.unbounded<AgentControlStageRunLeaseEvent>();
        const workerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void));
        const worker = yield* buildRuntime(
          filename,
          workerScope,
          {
            ...defaultHooks,
            beforeTransaction: (handoffId) =>
              handoffId === "handoff-worker-late"
                ? Deferred.succeed(laterAttempted, undefined).pipe(Effect.asVoid)
                : Effect.void,
            afterAuthoritativeRead: (handoffId) =>
              handoffId === raceSource.handoffId
                ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release)))
                : Effect.void,
          },
          Stream.fromPubSub(leaseEvents),
        );
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        yield* worker.finalizer.prepare(Effect.void).pipe(Scope.provide(ownerScope));
        yield* Deferred.await(reached);

        const writerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(writerScope, Exit.void));
        const writer = yield* buildRuntime(filename, writerScope);
        const changedDraft: AgentControlTaskEventDraft = {
          eventId: EventId.make("task-worker-source-gate-race"),
          type: "agentControl.task.sourceGate.changed",
          aggregateKind: "task",
          aggregateId: raceSource.taskId,
          occurredAt: sourceChangedAt,
          commandId: CommandId.make("task-worker-source-gate-race-command"),
          causationEventId: null,
          correlationId: CommandId.make("task-worker-source-gate-race-command"),
          authority: "controller",
          metadata: { schemaVersion: 1 },
          payload: {
            taskId: raceSource.taskId,
            source: raceDraft.payload.source,
            previousSourceGate: "eligible",
            sourceGate: "not-ready",
            sourceUpdatedAt: sourceChangedAt,
            githubIntakeSequence: 2,
            sourceSnapshot: {
              ...raceDraft.payload.sourceSnapshot,
              updatedAt: sourceChangedAt,
              ready: false,
              eligible: false,
              eligibilityReason: "ready-inactive",
            },
            changedAt: sourceChangedAt,
          },
        };
        const changed = yield* writer.events.append({
          taskId: raceSource.taskId,
          expectedStreamVersion: 1,
          events: [changedDraft],
        });
        yield* writer.projection.projectEvent(changed[0]!);
        yield* Deferred.succeed(release, undefined);
        yield* worker.finalizer.drain;

        const afterRetry = Option.getOrThrow(yield* setup.states.get(raceSource.taskId));
        assert.equal(afterRetry.revision, 3);
        assert.equal(afterRetry.sourceGate, "not-ready");
        assert.equal(afterRetry.stage, "verification");
        assert.equal(yield* Ref.get(worker.publications), 1);

        yield* seedTask(writer, "worker-late");
        const laterSource = seedCommittedVerificationFinalization(
          filename,
          "worker-late",
          "passed",
        );
        yield* PubSub.publish(leaseEvents, laterSource.leaseEvent);
        yield* Deferred.await(laterAttempted);
        yield* worker.finalizer.drain;

        const later = Option.getOrThrow(yield* setup.states.get(laterSource.taskId));
        assert.equal(later.revision, 2);
        assert.equal(later.stage, "verification");
        assert.equal(yield* Ref.get(worker.publications), 2);
        assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
          { evidence: 2, events: 2, markers: 2, publications: 2, receipts: 2 },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "isolates a twice-failed typed startup candidate and processes a later lease event on the same worker",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "task-verification-finalizer-worker-typed-failure-",
        });
        const filename = `${directory}/state.sqlite`;
        const setupScope = yield* Scope.make("sequential");
        const setup = yield* buildRuntime(filename, setupScope);
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, setup.sql),
        );
        yield* seedTask(setup, "worker-typed-failure");
        const failedSource = seedCommittedVerificationFinalization(
          filename,
          "worker-typed-failure",
          "passed",
        );
        yield* Scope.close(setupScope, Exit.void);

        const attempts = yield* Ref.make(0);
        const secondFailureAttempted = yield* Deferred.make<void>();
        const laterAttempted = yield* Deferred.make<void>();
        const leaseEvents = yield* PubSub.unbounded<AgentControlStageRunLeaseEvent>();
        const workerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(workerScope, Exit.void));
        const worker = yield* buildRuntime(
          filename,
          workerScope,
          {
            ...defaultHooks,
            beforeTransaction: (handoffId) =>
              handoffId === failedSource.handoffId
                ? Ref.updateAndGet(attempts, (count) => count + 1).pipe(
                    Effect.flatMap((count) =>
                      count === 2
                        ? Deferred.succeed(secondFailureAttempted, undefined)
                        : Effect.void,
                    ),
                  )
                : Deferred.succeed(laterAttempted, undefined).pipe(Effect.asVoid),
          },
          Stream.fromPubSub(leaseEvents),
        );
        yield* worker.sql.unsafe(`
          CREATE TEMP TRIGGER fail_worker_typed_candidate
          BEFORE INSERT ON main.agent_control_task_verification_finalization_evidence
          WHEN NEW.handoff_id = 'handoff-worker-typed-failure'
          BEGIN SELECT RAISE(ABORT, 'injected typed worker persistence failure'); END
        `).unprepared;
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        yield* worker.finalizer.prepare(Effect.void).pipe(Scope.provide(ownerScope));
        yield* Deferred.await(secondFailureAttempted);
        yield* worker.finalizer.drain;

        assert.equal(yield* Ref.get(attempts), 2);
        assert.equal(yield* Ref.get(worker.publications), 0);
        assert.deepStrictEqual(yield* finalizationCounts(worker.sql), [
          { evidence: 0, events: 0, markers: 0, publications: 0, receipts: 0 },
        ]);
        const directFailure = yield* Effect.result(
          worker.finalizer.processHandoff(failedSource.handoffId),
        );
        assert.equal(directFailure._tag, "Failure");
        if (directFailure._tag === "Failure") {
          assert.equal(directFailure.failure.operation, "insert-evidence");
          assert.equal(directFailure.failure.reason, "persistence");
        }

        yield* seedTask(worker, "worker-after-typed-failure");
        const laterSource = seedCommittedVerificationFinalization(
          filename,
          "worker-after-typed-failure",
          "passed",
        );
        yield* PubSub.publish(leaseEvents, laterSource.leaseEvent);
        yield* Deferred.await(laterAttempted);
        yield* worker.finalizer.drain;

        assert.equal(
          Option.getOrThrow(yield* worker.states.get(laterSource.taskId)).stage,
          "verification",
        );
        assert.equal(yield* Ref.get(worker.publications), 1);
        assert.deepStrictEqual(yield* finalizationCounts(worker.sql), [
          { evidence: 1, events: 1, markers: 1, publications: 1, receipts: 1 },
        ]);
        yield* Scope.close(ownerScope, Exit.void);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live("keeps a prepared worker defect observable with its original Cause", () =>
  withDatabase("task-verification-finalizer-worker-defect-", (_filename, runtime) =>
    Effect.gen(function* () {
      yield* seedTask(runtime, "worker-defect");
      seedCommittedVerificationFinalization(_filename, "worker-defect", "passed");
      const failure = new Error("injected worker defect");
      const defectScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(defectScope, Exit.void));
      const defectRuntime = yield* buildRuntime(_filename, defectScope, {
        ...defaultHooks,
        beforeTransaction: () => Effect.die(failure),
      });
      const ownerScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
      yield* defectRuntime.finalizer.prepare(Effect.void).pipe(Scope.provide(ownerScope));
      const drain = yield* Effect.exit(defectRuntime.finalizer.drain);
      assert.isTrue(Exit.isFailure(drain));
      if (Exit.isFailure(drain)) {
        assert.isTrue(Cause.hasDies(drain.cause));
        assert.include(Cause.pretty(drain.cause), failure.message);
      }
    }),
  ),
);
