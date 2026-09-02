import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEventDraft,
  type AgentControlWorktreeEventDraft,
} from "@t3tools/contracts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  loadAgentControlImplementationTaskAuthorityInTransaction,
  loadAgentControlImplementationWorktreeAuthorityInTransaction,
} from "../../agentControl/implementationTurn/historicalAuthority.ts";
import { canonicalJson } from "../../agentControl/initialPlanning/eventEvidence.ts";
import { fingerprintRunOnceModeCommand } from "../../agentControl/runOnce/authority.ts";
import { deriveAgentControlRunOnceId } from "../../agentControl/runOnce/identity.ts";
import { fingerprintAgentControlRunOnceSource } from "../../agentControl/runOnce/source.ts";
import { layer as AgentControlTaskEventStoreLive } from "../../agentControl/task/Layers/AgentControlTaskEventStore.ts";
import { layer as AgentControlTaskProjectionLive } from "../../agentControl/task/Layers/AgentControlTaskProjection.ts";
import { layer as AgentControlTaskStateRepositoryLive } from "../../agentControl/task/Layers/AgentControlTaskStateRepository.ts";
import { AgentControlTaskEventStore } from "../../agentControl/task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskProjection } from "../../agentControl/task/Services/AgentControlTaskProjection.ts";
import { AgentControlTaskStateRepository } from "../../agentControl/task/Services/AgentControlTaskStateRepository.ts";
import {
  loadAgentControlVerificationTaskAuthorityInTransaction,
  loadAgentControlVerificationWorktreeAuthorityInTransaction,
} from "../../agentControl/verificationTurn/historicalAuthority.ts";
import {
  deriveAgentControlWorktreePathKeys,
  deriveAgentControlWorktreeReservationId,
} from "../../agentControl/worktree/identity.ts";
import { layer as AgentControlWorktreeEventStoreLive } from "../../agentControl/worktree/Layers/AgentControlWorktreeEventStore.ts";
import { layer as AgentControlWorktreeProjectionLive } from "../../agentControl/worktree/Layers/AgentControlWorktreeProjection.ts";
import { layer as AgentControlWorktreeStateRepositoryLive } from "../../agentControl/worktree/Layers/AgentControlWorktreeStateRepository.ts";
import { AgentControlWorktreeEventStore } from "../../agentControl/worktree/Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeProjection } from "../../agentControl/worktree/Services/AgentControlWorktreeProjection.ts";
import { AgentControlWorktreeStateRepository } from "../../agentControl/worktree/Services/AgentControlWorktreeStateRepository.ts";
import { AgentControlProjectionStateRepositoryLive } from "../Layers/AgentControlProjectStates.ts";
import { AgentControlProjectionStateRepository } from "../Services/AgentControlProjectStates.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import {
  makeMigration064,
  type Migration064FaultPoint,
} from "./064_AgentControlArmedSingleFlight.ts";

const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

const at = "2026-09-02T08:00:00.000Z";
const projectId = ProjectId.make("migration-064-populated-project");
const source063DdlFingerprints = {
  idx_agent_control_project_states_sequence:
    "a51422bede7c7fc118944b687dabf9aac368bdfd8b1bae036d0a8c284cfb52e8",
  idx_agent_control_run_once_candidates:
    "27641587cf24c0b2fa2cc5afe80071c1bdd4351abedea7716890eab2154bf566",
  agent_control_project_states: "c37258c881c09d1bf1790fdf7909beae0ff83baf286dbcc9f0ba08cca5a90ae5",
  agent_control_run_once_activations:
    "c31369d53e016f6b12d3a212d7750dc3d974b41a61d31c0ae703691b0201f133",
  agent_control_run_once_activation_event_validate:
    "fce4a5f63e86792110e12325798dd89648a9849442b7e8bb21cb89e7ad336793",
  agent_control_run_once_activation_project_validate:
    "77c9c0a7a36e5da73cfbe52bda4f64bfd11d687219e2991d827b3209bd968d03",
  agent_control_run_once_active_lineage_validate:
    "3cb0ddd2f6431a2343fde78d678da7a3d74a64b61b31154dd08011b71f7342d1",
  agent_control_run_once_evidence_validate:
    "3897023c2e3070412d0f6fc3715a4767fc85c3c8e1cf1700440a20090ef984c2",
  agent_control_run_once_mode_evidence_validate:
    "cb0ddfa8f3a028a1d6e130f83a687de29957baa1af09ba7051b930643128f6cb",
} as const;
const target064DdlFingerprints = {
  agent_control_armed_dispatch_evidence:
    "94563d53f9bfb93e4a105c019e88d2306a9b9910c36cea7c125d1e08ee5a57c9",
  agent_control_armed_dispatch_evidence_no_delete:
    "a5d127e751e0d71cd299545084d08458c9143c21bbd0ebf70de37decfec94dbb",
  agent_control_armed_dispatch_evidence_no_update:
    "5bb3ab83f19690f32baa76878098cd3972b2ce6fb277a5aa8a2074cc4ed80e75",
  agent_control_armed_dispatch_evidence_validate:
    "49543c18fd009c58ef3887e81aeb1de91dc6d9ae064b6b4f8de27a15b1766fbd",
  agent_control_armed_dispatch_marker_validate:
    "57f39da1a2ef75dcea2d7714e2ce3b72abe59bd60bf7d02dd6bb8d45c7b5e492",
  agent_control_armed_dispatch_markers:
    "f9ef7310958c93cee912d5ba4c74df5109f07cc62aca604073bd5f2b8514c00f",
  agent_control_armed_dispatch_markers_no_delete:
    "7ff69734328292f7d2a51d2c389f69e1d4b4ad5d597b04dc23af0549401cd67c",
  agent_control_armed_dispatch_markers_no_update:
    "e74564f461a499060feafb8bcceae6a332708674cb7d190fb33695a768e146d9",
  agent_control_armed_dispatch_receipt_validate:
    "47554931ad0e8ff5dea5705c987b8d8c02285ff836b634464a53c4d0497f2960",
  agent_control_armed_dispatch_receipts:
    "2f22606e265feee4f25f30d78ebb0fa8c2ca14c3d4b8e43106098e97a27ee0f0",
  agent_control_armed_dispatch_receipts_no_delete:
    "212718d11365332ec272da6b9730c98f5b0ca47ac7f4a768dad55d2cb1822e92",
  agent_control_armed_dispatch_receipts_no_update:
    "e732435d8be8da263ebef80774f97f26ba1e62baedcf594abaaf198931f94eaf",
  agent_control_armed_dispatch_state_insert_validate:
    "e64a354025497cb58f1b4592e7475e981d77fbadcb6102c79d3c7311cd34b526",
  agent_control_armed_dispatch_state_no_delete:
    "4cd176c60648cc957a565c130601685a93bcd15991576db546c235ebbc19371e",
  agent_control_armed_dispatch_state_update_validate:
    "231886f5b8551788797a100a8c43f1347da84bdee30e18f67a560fd370750a56",
  agent_control_armed_dispatch_states:
    "5a9e0ec2fc2e4f7082f54de8a185abdea2468729d551c59bef773ee55bb4fada",
  agent_control_armed_no_candidate_evidence:
    "3b1ce0cf25d2ac231cde248f46ac1736a612bb9a0c329452d3cdb6de4af3983c",
  agent_control_armed_no_candidate_evidence_no_delete:
    "5105238edd5a26136d97ca15356d413b3cd9ba0a7dceabdd7e25fbbc3b521c1a",
  agent_control_armed_no_candidate_evidence_no_update:
    "98a186ecce3549df1bfcf4d684dc11f9361509447b05222e37ed0fbc581c2540",
  agent_control_armed_no_candidate_evidence_validate:
    "762f24eb6a34ad1496b264a07eaea2faa75fd4920ded6e2da00fd65d881437cf",
  agent_control_armed_no_candidate_marker_validate:
    "57f533c706bdb3f92c411f94941683dd52de6e4e4c4a886b1980ce3dbc8fee02",
  agent_control_armed_no_candidate_markers:
    "d15db2a61c2e7d5242664bfe327f3066c7567a14a84d47fa589a70a536c4ea18",
  agent_control_armed_no_candidate_markers_no_delete:
    "29d18f01815eac703fc136bc1549c69caba0c11d0a8b982def1505a38a68d478",
  agent_control_armed_no_candidate_markers_no_update:
    "b53d0a7a46beb6d796891efa2ac54251f8c05f9d975267a08c77289b8d6b09ba",
  agent_control_armed_no_candidate_receipt_validate:
    "f2bd4200fd7c484de5ff8aae4da43b9a87ab0f812b3c443b625b612f43a347e9",
  agent_control_armed_no_candidate_receipts:
    "e0b3df4dcdae62440c4f8c7072b6b2ab594cc80f77b12391779d584cc454a7a6",
  agent_control_armed_no_candidate_receipts_no_delete:
    "92b60cd8b41c3ced2c6b0a71391f645d557facfa4c7b5c5cc4c8f16784d0fe6a",
  agent_control_armed_no_candidate_receipts_no_update:
    "24c46012bc94f5355fc3f4a1bf8c12af73ecbed17eea51eab734383a3fa4f65b",
  agent_control_armed_system_activation_validate:
    "0ef1a7cc2ab74395176ef9c40662b497244a3d5d0cef176d3b72df4c32333baa",
  agent_control_project_states: "4db227bf824894ae65b802e620a816e9e0937a7b292eb40109d41be77e8dd214",
  agent_control_run_once_activation_event_validate:
    "dcf7804b2c547c476300bb3dc234d28cf282e25679aabeed96c51d55d4491d40",
  agent_control_run_once_activation_project_validate:
    "6333bdaf5d4a77116cab7d14e9b4b277ad05c0653a3377c9704fc838db12c673",
  agent_control_run_once_activations:
    "544f6d969779738410b9533e8de03e694095bc4920cfa181f415458b8bde4ec6",
  agent_control_run_once_evidence_validate:
    "a5853dd773aefe5597de967e79268d52f1f65253b55a1b4ad40f9e5e3925e49d",
  agent_control_run_once_mode_evidence_validate:
    "40d31ca32425d7fe6a0799e3c34905b4517578871871d85d34c67888cc63e577",
  agent_control_run_once_system_reset_origin_validate:
    "c536c511d5fde49a56e7e8f2dc9a42aa2a08bd8ca0a3a3f0b274e8b020c6f1a4",
  idx_agent_control_armed_active_project:
    "cb4fb076a1e5ee407f53bdbd49a2e67ee0c2395b03628f9e8a5b52c4f672c70d",
  idx_agent_control_armed_claim_recovery:
    "f6e0f166d0b7f357b72476fb2669fa930ce45154d790074e59fc80bcdf9e6602",
  idx_agent_control_armed_no_candidate_catchup:
    "9f228d151695ca3d73fbea3adc8429a2002442fe42b05ab3ebf92e36a4d3049e",
  idx_agent_control_armed_project_catchup:
    "c019578453e1fb52124a0a9f6a3f3aba0fabe5e167dd8136bf9f8612739e5d26",
  idx_agent_control_armed_task_frontier:
    "358f881044e5533103d1b1abdd3294406f57ddd61a8058811dadea16555ac892",
  idx_agent_control_run_once_armed_dispatch:
    "04959396aeb8fcab4f8ec9a0f4318053dcdb10ba9ef2d60fadea173867206b30",
} as const;

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const seedLegacyHistoricalAuthority063 = Effect.fn("seedLegacyHistoricalAuthority063")(function* (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
) {
  const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
  const taskEventContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlTaskEventStoreLive).pipe(Layer.provide(sqlLayer)),
    scope,
  );
  const taskStateContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlTaskStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
    scope,
  );
  const taskEvents = Context.get(taskEventContext, AgentControlTaskEventStore);
  const taskStates = Context.get(taskStateContext, AgentControlTaskStateRepository);
  const taskProjectionContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlTaskProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlTaskEventStore, taskEvents),
          Layer.succeed(AgentControlTaskStateRepository, taskStates),
        ),
      ),
    ),
    scope,
  );
  const taskProjection = Context.get(taskProjectionContext, AgentControlTaskProjection);
  const worktreeEventContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlWorktreeEventStoreLive).pipe(Layer.provide(sqlLayer)),
    scope,
  );
  const worktreeStateContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlWorktreeStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
    scope,
  );
  const projectionStateContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
    scope,
  );
  const worktreeEvents = Context.get(worktreeEventContext, AgentControlWorktreeEventStore);
  const worktreeStates = Context.get(worktreeStateContext, AgentControlWorktreeStateRepository);
  const worktreeProjectionContext = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlWorktreeProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlWorktreeEventStore, worktreeEvents),
          Layer.succeed(AgentControlWorktreeStateRepository, worktreeStates),
          Layer.succeed(
            AgentControlProjectionStateRepository,
            Context.get(projectionStateContext, AgentControlProjectionStateRepository),
          ),
        ),
      ),
    ),
    scope,
  );
  const worktreeProjection = Context.get(worktreeProjectionContext, AgentControlWorktreeProjection);

  yield* sql`
    INSERT INTO main.projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Migration 064 legacy history', '/tmp/migration-064-history',
      NULL, '[]', ${at}, ${at}, NULL
    )
  `;
  const taskId = AgentControlTaskId.make("migration-064-legacy-task");
  const taskEventId = EventId.make("migration-064-legacy-task-event");
  const taskCommandId = CommandId.make("migration-064-legacy-task-command");
  const taskDraft = {
    eventId: taskEventId,
    type: "agentControl.task.created",
    aggregateKind: "task",
    aggregateId: taskId,
    occurredAt: at,
    commandId: taskCommandId,
    causationEventId: null,
    correlationId: taskCommandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      taskId,
      source: {
        projectId,
        repositoryNodeId: "migration-064-repository",
        issueNodeId: "migration-064-legacy-issue",
        issueNumber: 64,
        issueUrl: "https://example.test/owner/repo/issues/64",
      },
      status: "candidate",
      sourceGate: "eligible",
      stage: "intake",
      sourceUpdatedAt: at,
      githubIntakeSequence: 3,
      sourceSnapshot: {
        repositoryNodeId: "migration-064-repository",
        issueNodeId: "migration-064-legacy-issue",
        number: 64,
        url: "https://example.test/owner/repo/issues/64",
        state: "open",
        title: "Migration 064 legacy authority",
        body: null,
        contentTrust: "untrusted-external",
        updatedAt: at,
        timelineComplete: true,
        ready: true,
        paused: false,
        eligible: true,
        eligibilityReason: "eligible",
      },
      createdAt: at,
    },
  } satisfies AgentControlTaskEventDraft;
  const [taskEvent] = yield* taskEvents.append({
    taskId,
    expectedStreamVersion: 0,
    events: [taskDraft],
  });
  yield* taskProjection.projectEvent(taskEvent!);

  const stageRunId = AgentControlStageRunId.make("migration-064-legacy-stage");
  const attemptId = AgentControlAttemptId.make("migration-064-legacy-attempt");
  const leaseId = AgentControlStageRunLeaseId.make("migration-064-legacy-lease");
  const baseCommitSha = "b".repeat(40);
  const targetGenerationId = "c".repeat(64);
  const reservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId,
    taskId,
    stageRunId,
    attemptId,
    leaseId,
    fenceToken: 1,
    repositoryIdentity: {
      repositoryNodeId: "migration-064-repository",
      canonicalKey: "github.com/owner/repo",
    },
    baseCommitSha,
  });
  const pathKeys = deriveAgentControlWorktreePathKeys({
    projectId,
    reservationId,
    targetGenerationId,
  });
  const worktreeEventId = EventId.make("migration-064-legacy-worktree-event");
  const worktreeCommandId = CommandId.make("migration-064-legacy-worktree-command");
  const worktreeDraft = {
    eventId: worktreeEventId,
    type: "agentControl.worktree.reserved",
    aggregateKind: "worktree-reservation",
    aggregateId: reservationId,
    occurredAt: at,
    commandId: worktreeCommandId,
    causationEventId: null,
    correlationId: worktreeCommandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      reservationId,
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 3,
      sourceIdentityFingerprint: "a".repeat(64),
      stageRunId,
      attemptId,
      leaseId,
      fenceToken: 1,
      repository: {
        repositoryNodeId: "migration-064-repository",
        nameWithOwner: "owner/repo",
        canonicalKey: "github.com/owner/repo",
        remoteName: "origin",
        remoteUrl: "github.com/owner/repo",
        defaultRemoteRef: "refs/remotes/origin/main",
        commonDirDevice: 1,
        commonDirInode: 1,
      },
      repositoryWorkspace: "/tmp/migration-064-history/repository",
      repositoryCommonDir: "/tmp/migration-064-history/repository/.git",
      baseRef: "origin/main",
      baseCommitSha,
      branchName: "t3auto/issue-64-migration-history",
      internalWorktreePath: `/tmp/migration-064-history/${pathKeys.reservationKey}-${pathKeys.generationKey}`,
      targetGenerationId,
      worktreeRootDevice: 1,
      worktreeRootInode: 1,
      worktreeParentDevice: 1,
      worktreeParentInode: 1,
      reservedAt: at,
    },
  } satisfies AgentControlWorktreeEventDraft;
  const [worktreeEvent] = yield* worktreeEvents.append({
    reservationId,
    expectedStreamVersion: 0,
    events: [worktreeDraft],
  });
  yield* worktreeProjection.projectEvent(worktreeEvent!);

  return {
    taskId,
    reservationId,
    taskEventId,
    worktreeEventId,
    taskLegacyPayload: encodeUnknownJson(taskDraft.payload),
    worktreeLegacyPayload: encodeUnknownJson(worktreeDraft.payload),
    taskCanonicalPayload: canonicalJson(taskDraft.payload as never),
    worktreeCanonicalPayload: canonicalJson(worktreeDraft.payload as never),
  } as const;
});

const seedPopulated063 = Effect.fn("seedPopulated063")(function* (sql: SqlClient.SqlClient) {
  const insertEvent = Effect.fn("insertMigration064Event")(function* (input: {
    readonly eventId: string;
    readonly aggregateKind: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: Record<string, unknown>;
  }) {
    return (yield* sql<{ readonly sequence: number }>`
      INSERT INTO main.agent_control_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, causation_event_id, correlation_id,
        actor_authority, payload_json, metadata_json
      ) VALUES (
        ${input.eventId}, ${input.aggregateKind}, ${projectId}, ${input.streamVersion},
        ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
        ${input.authority}, ${canonicalJson(input.payload as never)}, '{"schemaVersion":1}'
      ) RETURNING sequence
    `)[0]!.sequence;
  });

  yield* insertEvent({
    eventId: "migration-064-observe-event",
    aggregateKind: "project-controller",
    streamVersion: 1,
    eventType: "agentControl.project.mode.changed",
    commandId: "migration-064-observe-command",
    authority: "human",
    payload: {
      projectId,
      previousMode: "manual",
      mode: "observe",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  yield* insertEvent({
    eventId: "migration-064-github-config-event",
    aggregateKind: "github-intake",
    streamVersion: 1,
    eventType: "agentControl.github.config.set",
    commandId: "migration-064-github-config-command",
    authority: "human",
    payload: {
      projectId,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      repository: { repositoryNodeId: "migration-064-repository", nameWithOwner: "owner/repo" },
      configuredAt: at,
    },
  });
  const githubSequence = yield* insertEvent({
    eventId: "migration-064-github-success-event",
    aggregateKind: "github-intake",
    streamVersion: 2,
    eventType: "agentControl.github.poll.succeeded",
    commandId: "migration-064-github-success-command",
    authority: "controller",
    payload: {
      projectId,
      repository: { repositoryNodeId: "migration-064-repository", nameWithOwner: "owner/repo" },
      attemptedAt: at,
      completedAt: at,
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
      issues: [],
    },
  });
  const activationCommandId = CommandId.make("migration-064-run-once-command");
  const activationEventId = EventId.make("migration-064-run-once-event");
  const activationPayload = {
    projectId,
    previousMode: "observe",
    mode: "run-once",
    previousPausedFromMode: null,
    pausedFromMode: null,
    changedAt: at,
  } as const;
  const activationSequence = yield* insertEvent({
    eventId: activationEventId,
    aggregateKind: "project-controller",
    streamVersion: 2,
    eventType: "agentControl.project.mode.changed",
    commandId: activationCommandId,
    authority: "human",
    payload: activationPayload,
  });
  const commandFingerprint = fingerprintRunOnceModeCommand({
    commandId: activationCommandId,
    projectId,
    expectedRevision: 1,
    mode: "run-once",
  });
  yield* sql`
    INSERT INTO main.agent_control_command_receipts (
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created, accepted_at, error_code
    ) VALUES (
      ${activationCommandId}, ${commandFingerprint}, 'human', 'project-controller', ${projectId},
      'accepted', ${activationSequence}, 2, 1, ${at}, NULL
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'run-once', NULL, 2, ${activationSequence}, ${at})
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence, revision, status, updated_at
    ) VALUES (${projectId}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
  `;
  const runId = deriveAgentControlRunOnceId({
    projectId,
    activationEventId,
    activationEventSequence: activationSequence,
    activationEventStreamVersion: 2,
    activationCommandId,
  });
  const sourceFingerprint = fingerprintAgentControlRunOnceSource({
    schemaVersion: 1,
    projectId,
    githubIntakeSequence: githubSequence,
    githubProjectionRevision: 2,
    githubConfigRevision: 2,
    repositoryNodeId: "migration-064-repository",
    pollStatus: "success",
    expectedIssueCount: 0,
  });
  const payload = new TextEncoder().encode(canonicalJson(activationPayload as never));
  const metadata = new TextEncoder().encode('{"schemaVersion":1}');
  yield* sql`
    INSERT INTO main.agent_control_run_once_activations (
      run_id, project_id, activation_event_id, activation_event_sequence,
      activation_event_stream_version, activation_command_id,
      activation_expected_revision, activation_command_fingerprint,
      activation_event_payload_json, activation_event_metadata_json,
      github_intake_sequence, github_event_id, github_event_sequence,
      github_event_stream_version, reconcile_revision, source_fingerprint, activated_at
    ) VALUES (
      ${runId}, ${projectId}, ${activationEventId}, ${activationSequence}, 2,
      ${activationCommandId}, 1, ${commandFingerprint}, ${payload}, ${metadata},
      ${githubSequence}, 'migration-064-github-success-event', ${githubSequence}, 2, 1,
      ${sourceFingerprint}, ${at}
    )
  `;
  return runId;
});

it.live("installs Armed authority on a fresh database and exposes it across WAL", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-064-fresh-" });
      const filename = path.join(directory, "fresh.sqlite");
      const writer = yield* openDatabase(filename);
      const observer = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(writer.scope, Exit.void));

      const applied = yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
        Effect.provideService(SqlClient.SqlClient, writer.sql),
      );
      assert.equal(applied.length, 64);
      assert.deepStrictEqual(applied.at(-1), [64, "AgentControlArmedSingleFlight"]);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql<{ readonly name: string }>`
          SELECT name FROM main.sqlite_schema
          WHERE type = 'table' AND name LIKE 'agent_control_armed_%'
          ORDER BY name
        `,
        [
          { name: "agent_control_armed_dispatch_evidence" },
          { name: "agent_control_armed_dispatch_markers" },
          { name: "agent_control_armed_dispatch_receipts" },
          { name: "agent_control_armed_dispatch_states" },
          { name: "agent_control_armed_no_candidate_evidence" },
          { name: "agent_control_armed_no_candidate_markers" },
          { name: "agent_control_armed_no_candidate_receipts" },
        ],
      );
      const installed = yield* observer.sql<{
        readonly name: string;
        readonly sql: string;
        readonly type: string;
      }>`
        SELECT type, name, sql FROM main.sqlite_schema
        WHERE (
          name LIKE 'agent_control_armed_%'
          OR name IN (
             'idx_agent_control_armed_active_project',
             'idx_agent_control_armed_claim_recovery',
             'idx_agent_control_armed_no_candidate_catchup',
             'idx_agent_control_armed_project_catchup',
             'idx_agent_control_armed_task_frontier'
          )
        ) AND sql IS NOT NULL
        ORDER BY type, name
      `;
      assert.lengthOf(installed, 34);
      assert.isTrue(installed.every((object) => typeof object.sql === "string"));
      assert.isTrue(
        installed
          .filter((object) => object.type === "table")
          .every((object) => object.sql.includes("typeof(")),
      );
      const projectStateDdl = (yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table' AND name = 'agent_control_project_states'
      `)[0]!.sql;
      assert.include(projectStateDdl, "'armed'");
      assert.include(projectStateDdl, "paused_from_mode IN ('observe', 'armed', 'run-once')");
      const activationColumns = yield* observer.sql<{
        readonly name: string;
        readonly notnull: number;
        readonly type: string;
      }>`
        SELECT name, type, "notnull" FROM pragma_table_xinfo('agent_control_run_once_activations')
        WHERE name IN ('armed_dispatch_id', 'armed_claim_id', 'armed_marker_id', 'origin_mode')
        ORDER BY name
      `;
      assert.deepStrictEqual(activationColumns, [
        { name: "armed_claim_id", type: "TEXT", notnull: 0 },
        { name: "armed_dispatch_id", type: "TEXT", notnull: 0 },
        { name: "armed_marker_id", type: "TEXT", notnull: 0 },
        { name: "origin_mode", type: "TEXT", notnull: 1 },
      ]);
      const noCandidateEpochIndexInfo = yield* observer.sql<{
        readonly name: string;
        readonly seqno: number;
      }>`
        SELECT seqno, name
        FROM pragma_index_info('sqlite_autoindex_agent_control_armed_no_candidate_evidence_4')
        ORDER BY seqno
      `;
      assert.deepStrictEqual(noCandidateEpochIndexInfo, [
        { seqno: 0, name: "project_id" },
        { seqno: 1, name: "github_intake_sequence" },
        { seqno: 2, name: "github_event_id" },
        { seqno: 3, name: "github_event_sequence" },
        { seqno: 4, name: "github_event_stream_version" },
        { seqno: 5, name: "source_fingerprint" },
        { seqno: 6, name: "reconcile_revision" },
        { seqno: 7, name: "task_frontier_sequence" },
        { seqno: 8, name: "task_frontier_revision" },
        { seqno: 9, name: "task_frontier_count" },
        { seqno: 10, name: "task_frontier_fingerprint" },
      ]);
      const targetObjects = yield* observer.sql<{ readonly name: string; readonly sql: string }>`
        SELECT name, sql FROM main.sqlite_schema
        WHERE sql IS NOT NULL AND (
          name LIKE 'agent_control_armed_%'
          OR name IN (
            'idx_agent_control_armed_active_project',
            'idx_agent_control_armed_claim_recovery',
            'idx_agent_control_armed_no_candidate_catchup',
            'idx_agent_control_armed_project_catchup',
            'idx_agent_control_armed_task_frontier',
            'idx_agent_control_run_once_armed_dispatch',
            'agent_control_project_states',
            'agent_control_run_once_activations',
            'agent_control_run_once_activation_event_validate',
            'agent_control_run_once_activation_project_validate',
            'agent_control_run_once_evidence_validate',
            'agent_control_run_once_mode_evidence_validate',
            'agent_control_run_once_system_reset_origin_validate'
          )
        ) ORDER BY name
      `;
      assert.deepStrictEqual(
        Object.fromEntries(
          targetObjects.map((object) => [
            object.name,
            NodeCrypto.createHash("sha256").update(object.sql).digest("hex"),
          ]),
        ),
        target064DdlFingerprints,
      );
      assert.deepStrictEqual(yield* observer.sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* observer.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "upgrades populated 063 atomically, preserves historical bytes, and repairs after faults",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-064-upgrade-" });
        const filename = path.join(directory, "upgrade.sqlite");
        const writer = yield* openDatabase(filename);
        const observer = yield* openDatabase(filename);
        yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(writer.scope, Exit.void));
        yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
          Effect.provideService(SqlClient.SqlClient, writer.sql),
        );
        const runId = yield* seedPopulated063(writer.sql);
        const legacyHistory = yield* seedLegacyHistoricalAuthority063(writer.sql, writer.scope);
        const legacyHistoryBefore = yield* writer.sql<{
          readonly eventId: string;
          readonly aggregateKind: string;
          readonly payloadStorage: string;
          readonly payloadBytes: string;
          readonly payloadSource: string;
          readonly metadataStorage: string;
          readonly metadataBytes: string;
          readonly metadataSource: string;
        }>`
          SELECT event_id AS "eventId", aggregate_kind AS "aggregateKind",
            typeof(payload_json) AS "payloadStorage",
            hex(CAST(payload_json AS BLOB)) AS "payloadBytes",
            payload_json AS "payloadSource",
            typeof(metadata_json) AS "metadataStorage",
            hex(CAST(metadata_json AS BLOB)) AS "metadataBytes",
            metadata_json AS "metadataSource"
          FROM main.agent_control_events
          WHERE event_id IN (${legacyHistory.taskEventId}, ${legacyHistory.worktreeEventId})
          ORDER BY aggregate_kind
        `;
        assert.lengthOf(legacyHistoryBefore, 2);
        const legacyTaskRow = legacyHistoryBefore.find(
          (row) => row.eventId === legacyHistory.taskEventId,
        )!;
        const legacyWorktreeRow = legacyHistoryBefore.find(
          (row) => row.eventId === legacyHistory.worktreeEventId,
        )!;
        assert.deepStrictEqual(
          legacyHistoryBefore.map((row) => [
            row.aggregateKind,
            row.payloadStorage,
            row.metadataStorage,
          ]),
          [
            ["task", "text", "text"],
            ["worktree-reservation", "text", "text"],
          ],
        );
        assert.equal(legacyTaskRow.payloadSource, legacyHistory.taskLegacyPayload);
        assert.notEqual(legacyTaskRow.payloadSource, legacyHistory.taskCanonicalPayload);
        assert.equal(legacyWorktreeRow.payloadSource, legacyHistory.worktreeLegacyPayload);
        assert.notEqual(legacyWorktreeRow.payloadSource, legacyHistory.worktreeCanonicalPayload);
        assert.equal(legacyTaskRow.metadataSource, '{"schemaVersion":1}');
        assert.equal(legacyWorktreeRow.metadataSource, '{"schemaVersion":1}');
        const historyBefore = yield* writer.sql<Record<string, unknown>>`
        SELECT run_id AS "runId", project_id AS "projectId",
          activation_event_id AS "eventId", activation_event_sequence AS "eventSequence",
          activation_event_stream_version AS "eventVersion",
          activation_command_id AS "commandId",
          activation_expected_revision AS "expectedRevision",
          activation_command_fingerprint AS "commandFingerprint",
          typeof(activation_event_payload_json) AS "payloadStorage",
          hex(activation_event_payload_json) AS "payloadBytes",
          typeof(activation_event_metadata_json) AS "metadataStorage",
          hex(activation_event_metadata_json) AS "metadataBytes",
          github_intake_sequence AS "githubIntakeSequence",
          github_event_id AS "githubEventId", github_event_sequence AS "githubEventSequence",
          github_event_stream_version AS "githubEventVersion",
          reconcile_revision AS "reconcileRevision", source_fingerprint AS "sourceFingerprint",
          activated_at AS "activatedAt"
        FROM main.agent_control_run_once_activations WHERE run_id = ${runId}
      `;
        assert.lengthOf(historyBefore, 1);
        const schemaBefore = yield* writer.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
        assert.deepStrictEqual(
          Object.fromEntries(
            schemaBefore
              .filter(
                (object) =>
                  typeof object.name === "string" &&
                  (object.name === "agent_control_project_states" ||
                    object.name === "agent_control_run_once_activations" ||
                    object.name === "idx_agent_control_run_once_candidates" ||
                    (typeof object.sql === "string" &&
                      object.sql.includes("agent_control_project_states")) ||
                    (typeof object.name === "string" &&
                      [
                        "agent_control_run_once_activation_event_validate",
                        "agent_control_run_once_activation_project_validate",
                        "agent_control_run_once_evidence_validate",
                        "agent_control_run_once_mode_evidence_validate",
                      ].includes(object.name))) &&
                  typeof object.sql === "string",
              )
              .map((object) => [
                object.name,
                NodeCrypto.createHash("sha256")
                  .update(object.sql as string)
                  .digest("hex"),
              ]),
          ),
          source063DdlFingerprints,
        );

        const candidateIndex = schemaBefore.find(
          (object) => object.name === "idx_agent_control_run_once_candidates",
        );
        assert.equal(typeof candidateIndex?.sql, "string");
        yield* writer.sql`DROP INDEX main.idx_agent_control_run_once_candidates`;
        yield* writer.sql`
        CREATE INDEX main.idx_agent_control_run_once_candidates
        ON agent_control_task_states(project_id, task_id)
      `;
        const divergentSchema = yield* writer.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              writer.sql.withTransaction(
                makeMigration064().pipe(Effect.provideService(SqlClient.SqlClient, writer.sql)),
              ),
            ),
          ),
        );
        assert.deepStrictEqual(
          yield* writer.sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `,
          divergentSchema,
        );
        yield* writer.sql`DROP INDEX main.idx_agent_control_run_once_candidates`;
        yield* writer.sql.unsafe(candidateIndex!.sql as string).unprepared;
        assert.deepStrictEqual(
          yield* writer.sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `,
          schemaBefore,
        );

        for (const faultPoint of [
          "before-project-state-rebuild",
          "after-project-state-rebuild",
          "after-armed-tables",
          "after-authority-triggers",
        ] satisfies ReadonlyArray<Migration064FaultPoint>) {
          const result = yield* Effect.exit(
            writer.sql.withTransaction(
              makeMigration064((point) =>
                point === faultPoint ? Effect.die(new Error(`injected ${point}`)) : Effect.void,
              ).pipe(Effect.provideService(SqlClient.SqlClient, writer.sql)),
            ),
          );
          assert.isTrue(Exit.isFailure(result), faultPoint);
          assert.deepStrictEqual(
            yield* writer.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
            schemaBefore,
            faultPoint,
          );
        }

        yield* writer.sql`CREATE TABLE main.agent_control_armed_partial (value TEXT)`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              writer.sql.withTransaction(
                makeMigration064().pipe(Effect.provideService(SqlClient.SqlClient, writer.sql)),
              ),
            ),
          ),
        );
        yield* writer.sql`DROP TABLE main.agent_control_armed_partial`;
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
            Effect.provideService(SqlClient.SqlClient, writer.sql),
          ),
          [[64, "AgentControlArmedSingleFlight"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
            Effect.provideService(SqlClient.SqlClient, observer.sql),
          ),
          [],
        );
        assert.deepStrictEqual(
          yield* observer.sql<Record<string, unknown>>`
          SELECT run_id AS "runId", project_id AS "projectId",
            activation_event_id AS "eventId", activation_event_sequence AS "eventSequence",
            activation_event_stream_version AS "eventVersion",
            activation_command_id AS "commandId",
            activation_expected_revision AS "expectedRevision",
            activation_command_fingerprint AS "commandFingerprint",
            typeof(activation_event_payload_json) AS "payloadStorage",
            hex(activation_event_payload_json) AS "payloadBytes",
            typeof(activation_event_metadata_json) AS "metadataStorage",
            hex(activation_event_metadata_json) AS "metadataBytes",
            github_intake_sequence AS "githubIntakeSequence",
            github_event_id AS "githubEventId", github_event_sequence AS "githubEventSequence",
            github_event_stream_version AS "githubEventVersion",
            reconcile_revision AS "reconcileRevision", source_fingerprint AS "sourceFingerprint",
            activated_at AS "activatedAt"
          FROM main.agent_control_run_once_activations WHERE run_id = ${runId}
        `,
          historyBefore,
        );
        assert.deepStrictEqual(
          yield* observer.sql<Record<string, unknown>>`
            SELECT event_id AS "eventId", aggregate_kind AS "aggregateKind",
              typeof(payload_json) AS "payloadStorage",
              hex(CAST(payload_json AS BLOB)) AS "payloadBytes",
              payload_json AS "payloadSource",
              typeof(metadata_json) AS "metadataStorage",
              hex(CAST(metadata_json AS BLOB)) AS "metadataBytes",
              metadata_json AS "metadataSource"
            FROM main.agent_control_events
            WHERE event_id IN (${legacyHistory.taskEventId}, ${legacyHistory.worktreeEventId})
            ORDER BY aggregate_kind
          `,
          legacyHistoryBefore,
        );
        assert.equal(
          (yield* loadAgentControlImplementationTaskAuthorityInTransaction(
            observer.sql,
            legacyHistory.taskId,
            1,
          )).state.taskId,
          legacyHistory.taskId,
        );
        assert.equal(
          (yield* loadAgentControlImplementationWorktreeAuthorityInTransaction(
            observer.sql,
            legacyHistory.reservationId,
          )).state.reservationId,
          legacyHistory.reservationId,
        );
        assert.equal(
          (yield* loadAgentControlVerificationTaskAuthorityInTransaction(
            observer.sql,
            legacyHistory.taskId,
            1,
          )).state.taskId,
          legacyHistory.taskId,
        );
        assert.equal(
          (yield* loadAgentControlVerificationWorktreeAuthorityInTransaction(
            observer.sql,
            legacyHistory.reservationId,
          )).state.reservationId,
          legacyHistory.reservationId,
        );
        assert.deepStrictEqual(
          yield* observer.sql`
          SELECT origin_mode AS "originMode", armed_dispatch_id AS "dispatchId",
            armed_claim_id AS "claimId", armed_marker_id AS "markerId"
          FROM main.agent_control_run_once_activations WHERE run_id = ${runId}
        `,
          [{ originMode: "observe", dispatchId: null, claimId: null, markerId: null }],
        );
        const postInstallTargetObjects = yield* observer.sql<{
          readonly name: string;
          readonly sql: string;
        }>`
          SELECT name, sql FROM main.sqlite_schema
          WHERE sql IS NOT NULL AND (
            name LIKE 'agent_control_armed_%'
            OR name IN (
              'idx_agent_control_armed_active_project',
              'idx_agent_control_armed_claim_recovery',
              'idx_agent_control_armed_no_candidate_catchup',
              'idx_agent_control_armed_project_catchup',
              'idx_agent_control_armed_task_frontier',
              'idx_agent_control_run_once_armed_dispatch',
              'agent_control_project_states',
              'agent_control_run_once_activations',
              'agent_control_run_once_activation_event_validate',
              'agent_control_run_once_activation_project_validate',
              'agent_control_run_once_evidence_validate',
              'agent_control_run_once_mode_evidence_validate',
              'agent_control_run_once_system_reset_origin_validate'
            )
          ) ORDER BY name
        `;
        assert.deepStrictEqual(
          Object.fromEntries(
            postInstallTargetObjects.map((object) => [
              object.name,
              NodeCrypto.createHash("sha256").update(object.sql).digest("hex"),
            ]),
          ),
          target064DdlFingerprints,
        );

        for (const shadow of [
          "agent_control_project_states",
          "agent_control_armed_dispatch_evidence",
          "agent_control_armed_dispatch_states",
        ]) {
          yield* observer.sql.unsafe(`CREATE TEMP TABLE ${shadow}(shadow TEXT)`).unprepared;
        }
        assert.equal(
          (yield* observer.sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_armed_dispatch_states
        `)[0]!.count,
          0,
        );
        const canonical = '{"schemaVersion":1}';
        const fingerprint = "0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72";
        assert.deepStrictEqual(
          yield* observer.sql`
          SELECT
            t3_run_once_canonical_blob_match(CAST(${canonical} AS BLOB), ${fingerprint}) AS valid,
            t3_run_once_canonical_blob_match(${canonical}, ${fingerprint}) AS textValue,
            t3_run_once_canonical_blob_match(CAST('{ "schemaVersion":1}' AS BLOB), ${fingerprint})
              AS noncanonical
        `,
          [{ valid: 1, textValue: 0, noncanonical: 0 }],
        );
        const plans = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT candidate.task_id
        FROM main.agent_control_task_states AS candidate
        INDEXED BY idx_agent_control_run_once_candidates
        WHERE candidate.project_id = ${projectId}
          AND candidate.github_intake_sequence = 1
          AND candidate.status = 'candidate'
          AND candidate.source_gate = 'eligible'
          AND candidate.stage = 'intake'
        ORDER BY candidate.issue_number, candidate.task_id LIMIT 1
      `;
        assert.isTrue(
          plans.some((plan) => plan.detail.includes("idx_agent_control_run_once_candidates")),
        );
        assert.isFalse(plans.some((plan) => plan.detail.includes("TEMP B-TREE")));
        const frontierPlan = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT task_id AS "taskId", issue_number AS "issueNumber", status,
          source_gate AS "sourceGate", stage,
          github_intake_sequence AS "githubIntakeSequence", revision,
          last_event_sequence AS "lastEventSequence"
        FROM main.agent_control_task_states INDEXED BY idx_agent_control_armed_task_frontier
        WHERE project_id = ${projectId}
        ORDER BY task_id
      `;
        assert.isTrue(
          frontierPlan.some(
            (plan) =>
              plan.detail.includes("idx_agent_control_armed_task_frontier") &&
              plan.detail.includes("project_id=?"),
          ),
        );
        assert.isFalse(frontierPlan.some((plan) => plan.detail.includes("SCAN")));
        assert.isFalse(frontierPlan.some((plan) => plan.detail.includes("TEMP B-TREE")));
        const activeClaimPlan = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT dispatch_id AS "dispatchId", owner_id AS "ownerId", status,
          expires_at AS "expiresAt"
        FROM main.agent_control_armed_dispatch_states
        WHERE project_id = ${projectId} AND status IN ('claimed', 'activated')
      `;
        assert.isTrue(
          activeClaimPlan.some((plan) =>
            plan.detail.includes("idx_agent_control_armed_active_project"),
          ),
        );
        assert.isFalse(activeClaimPlan.some((plan) => plan.detail.includes("TEMP B-TREE")));
        const armedCatchUpPlan = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT project_id AS "projectId"
        FROM main.agent_control_project_states INDEXED BY idx_agent_control_armed_project_catchup
        WHERE mode = 'armed' AND paused_from_mode IS NULL
      `;
        assert.isTrue(
          armedCatchUpPlan.some(
            (plan) =>
              plan.detail.includes("idx_agent_control_armed_project_catchup") &&
              plan.detail.includes("mode=?"),
          ),
        );
        assert.isFalse(armedCatchUpPlan.some((plan) => plan.detail.includes("TEMP B-TREE")));
        const dispatchCatchUpPlan = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT project_id AS "projectId"
        FROM main.agent_control_armed_dispatch_states
          INDEXED BY idx_agent_control_armed_claim_recovery
        WHERE status IN ('claimed', 'activated')
      `;
        assert.isTrue(
          dispatchCatchUpPlan.some(
            (plan) =>
              plan.detail.includes("idx_agent_control_armed_claim_recovery") &&
              plan.detail.includes("status=?"),
          ),
        );
        assert.isFalse(dispatchCatchUpPlan.some((plan) => plan.detail.includes("TEMP B-TREE")));
        const noCandidatePlan = yield* observer.sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT evidence.evidence_id AS "evidenceId", receipt.receipt_id AS "receiptRecordId",
          marker.marker_id AS "markerRecordId"
        FROM main.agent_control_armed_no_candidate_evidence evidence
        LEFT JOIN main.agent_control_armed_no_candidate_receipts receipt
          ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
         AND receipt.marker_id = evidence.marker_id
        LEFT JOIN main.agent_control_armed_no_candidate_markers marker
          ON marker.evidence_id = evidence.evidence_id AND marker.marker_id = evidence.marker_id
         AND marker.receipt_id = evidence.receipt_id
        WHERE evidence.project_id = ${projectId}
          AND evidence.github_intake_sequence = 1
          AND evidence.github_event_id = 'armed-no-candidate-plan-probe'
          AND evidence.github_event_sequence = 1
          AND evidence.github_event_stream_version = 1
          AND evidence.source_fingerprint = ${"a".repeat(64)}
          AND evidence.reconcile_revision = 1
          AND evidence.task_frontier_sequence = 0
          AND evidence.task_frontier_revision = 0
          AND evidence.task_frontier_count = 0
          AND evidence.task_frontier_fingerprint = ${"b".repeat(64)}
      `;
        assert.isTrue(
          noCandidatePlan.some((plan) =>
            plan.detail.includes("sqlite_autoindex_agent_control_armed_no_candidate_evidence_4"),
          ),
        );
        assert.isFalse(noCandidatePlan.some((plan) => plan.detail.includes("SCAN")));
        assert.isFalse(noCandidatePlan.some((plan) => plan.detail.includes("TEMP B-TREE")));
        assert.deepStrictEqual(yield* observer.sql`PRAGMA main.foreign_key_check`, []);
        assert.equal((yield* observer.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
