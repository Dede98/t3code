// @effect-diagnostics nodeBuiltinImport:off - captures a committed fixture from a real child-process production harness.
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type AgentControlStageRunLeaseEvent,
  type AgentControlTaskEventDraft,
} from "@t3tools/contracts";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
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
  watchDirectory: string,
  snapshotFilename: string,
): Promise<void> =>
  new Promise((resolve, reject) => {
    const childEnvironment: NodeJS.ProcessEnv = {
      ...process.env,
      PATH: `/opt/homebrew/opt/node@24/bin:${process.env.PATH ?? ""}`,
      TMPDIR: watchDirectory,
    };
    delete childEnvironment.ELECTRON_RUN_AS_NODE;
    const output: Array<string> = [];
    let childDone = false;
    let childSucceeded = false;
    let captured = false;
    let lastCaptureError: unknown;
    const finish = () => {
      if (!childDone || !captured) return;
      watcher.close();
      if (childSucceeded) resolve();
      else reject(new Error(`production 061 fixture failed\n${output.join("")}`));
    };
    const inspectOnce = async () => {
      if (captured) return;
      for (const directory of NodeFS.readdirSync(watchDirectory, { withFileTypes: true })) {
        if (
          !directory.isDirectory() ||
          !directory.name.startsWith("t3-initial-planning-finalizer-")
        )
          continue;
        const filename = NodePath.join(watchDirectory, directory.name, "state.sqlite");
        if (!NodeFS.existsSync(filename)) continue;
        let database: NodeSqlite.DatabaseSync | undefined;
        try {
          database = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
          const row = database
            .prepare(`SELECT
              (SELECT count(*) FROM main.effect_sql_migrations WHERE migration_id = 61)
                AS migration,
              (SELECT count(*) FROM main.agent_control_verification_finalization_markers)
                AS markers`)
            .get() as { readonly markers: number; readonly migration: number };
          if (row.migration !== 1 || row.markers !== 1) continue;
          await NodeSqlite.backup(database, snapshotFilename);
          captured = true;
          finish();
          return;
        } catch (cause) {
          lastCaptureError = cause;
        } finally {
          database?.close();
        }
      }
    };
    let captureQueue = Promise.resolve();
    const inspect = () => {
      captureQueue = captureQueue.then(inspectOnce, inspectOnce);
      return captureQueue;
    };
    const watcher = NodeFS.watch(watchDirectory, { recursive: true }, () => void inspect());
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
      watcher.close();
      reject(cause);
    });
    child.once("close", (code) => {
      childDone = true;
      childSucceeded = code === 0;
      void inspect().finally(() => {
        if (!captured) {
          watcher.close();
          reject(
            new Error(
              `production 061 fixture was not captured${
                lastCaptureError === undefined ? "" : `: ${String(lastCaptureError)}`
              }\n${output.join("")}`,
            ),
          );
          return;
        }
        finish();
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
    const taskPubSub = yield* PubSub.unbounded<never>();
    const taskEngine = AgentControlTaskEngine.of({
      dispatchController: unavailable,
      dispatchObservedController: unavailable,
      get: unavailable,
      verifySourceSnapshot: unavailable,
      rebuild: unavailable(),
      publishCommitted: (committed) =>
        Effect.all([
          Ref.update(publications, (count) => count + committed.length),
          Ref.update(publishedEvents, (current) => [
            ...current,
            ...committed.map((event) => event.eventId),
          ]),
        ]).pipe(Effect.asVoid),
      streamDomainEvents: Stream.fromPubSub(taskPubSub),
      subscribeDomainEvents: Effect.succeed(Stream.fromPubSub(taskPubSub)),
    } satisfies AgentControlTaskEngineShape);
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
    } as const;
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
    readonly receipts: number;
  }>`
    SELECT
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_evidence) AS evidence,
      (SELECT count(*) FROM main.agent_control_events
        WHERE event_type = 'agentControl.task.finalizedAfterVerification') AS events,
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_markers) AS markers,
      (SELECT count(*) FROM main.agent_control_task_verification_finalization_receipts) AS receipts
  `;

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
              evidence.marker_id, evidence.finalization_fingerprint
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
      assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
        { evidence: 5, events: 5, markers: 5, receipts: 5 },
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
      assert.deepStrictEqual(yield* finalizationCounts(runtime.sql), [
        { evidence: 5, events: 5, markers: 5, receipts: 5 },
      ]);
    }),
  ),
);

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
        { evidence: 0, events: 0, markers: 0, receipts: 0 },
      ]);
      assert.equal(yield* Ref.get(failing.publications), 0);
      const stateBeforeRetry = Option.getOrThrow(yield* setup.states.get(source.taskId));
      assert.equal(stateBeforeRetry.revision, 1);
      assert.equal(stateBeforeRetry.stage, "intake");

      assert.equal((yield* setup.finalizer.processHandoff(source.handoffId))._tag, "Finalized");
      assert.deepStrictEqual(yield* finalizationCounts(setup.sql), [
        { evidence: 1, events: 1, markers: 1, receipts: 1 },
      ]);
      assert.equal(yield* Ref.get(setup.publications), 1);
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
        { evidence: 3, events: 3, markers: 3, receipts: 3 },
      ]);
      assert.equal(yield* Ref.get(restart.publications), 3);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
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
        { evidence: 0, events: 0, markers: 0, receipts: 0 },
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
          { evidence: 2, events: 2, markers: 2, receipts: 2 },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);
