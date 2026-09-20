import {
  AgentControlEpicAcceptedResult,
  AgentControlEpicFinalVerification,
  AgentControlVerificationChecks,
  AgentControlWorktreeReservationId,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicMemberView,
  type AgentControlWorktreeReservationState,
  AgentControlEpicRpcError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentControlGithubStateRepository } from "../github/Services/AgentControlGithubStateRepository.ts";
import { loadAuthoritativeWorktreeReservation } from "../worktree/authoritative.ts";
import { AgentControlWorktreeEventStore } from "../worktree/Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeStateRepository } from "../worktree/Services/AgentControlWorktreeStateRepository.ts";
import {
  assessVerificationChecks,
  VERIFICATION_CODE_SNAPSHOT_PREFIX,
  rawVerificationCodeDigest,
} from "../verificationTurn/checkEvidence.ts";
import { epicDigest, epicError, epicJson } from "./authority.ts";
import { sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import type { EpicHandoffRemoteAuthority } from "./remote.ts";

const isEpicError = Schema.is(AgentControlEpicRpcError);
const decodeChecks = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlVerificationChecks),
);
const invalid = (message: string) => epicError("handoff-evidence-invalid", message);
const decodeAccepted = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlEpicAcceptedResult),
);

const decodeIntegrated = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      accepted: AgentControlEpicAcceptedResult,
      verification: AgentControlEpicFinalVerification,
    }),
  ),
);
const decodeReviewRepairResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      status: Schema.Literals(["succeeded", "failed", "blocked"]),
      candidateCommitSha: Schema.NullOr(Schema.String),
      code: Schema.NullOr(Schema.String),
      message: Schema.NullOr(Schema.String),
    }),
  ),
);

export interface EpicHandoffProof {
  readonly authority: Omit<EpicHandoffRemoteAuthority, "branchName" | "ownershipToken">;
  readonly childCheckCount: number;
  readonly finalCheckCount: number;
}
export class EpicHandoffEvidence extends Context.Service<
  EpicHandoffEvidence,
  {
    readonly verify: (
      state: AgentControlEpicRuntimeView,
    ) => Effect.Effect<EpicHandoffProof, AgentControlEpicRpcError>;
  }
>()("t3/agentControl/epic/handoffAuthority/EpicHandoffEvidence") {}

/** Publication checks retained evidence only; mutable worktree files cannot become new evidence. */
export const makeEpicHandoffEvidence = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlWorktreeEventStore;
  const states = yield* AgentControlWorktreeStateRepository;
  const github = yield* AgentControlGithubStateRepository;
  const verifyOriginal = Effect.fn("EpicHandoffEvidence.verifyOriginal")(
    function* (state: AgentControlEpicRuntimeView) {
      const final = state.finalVerification;
      if (
        state.status !== "succeeded" ||
        !final ||
        final.status !== "passed" ||
        !state.acceptedCommitSha ||
        final.commitSha !== state.acceptedCommitSha ||
        final.evidenceId !== `epic-final:${state.epicRunId}:${state.verificationAttempt}` ||
        !state.finalVerificationHistory.some((item) => epicDigest(item) === epicDigest(final))
      )
        return yield* invalid(
          "This Epic has no valid successful verification of its accepted common commit.",
        );
      const required = state.checks.filter((check) => check.required);
      if (
        !required.length ||
        required.some(
          (check) =>
            !final.checks.some(
              (result) =>
                result.id === check.id && result.status === "passed" && result.exitCode === 0,
            ),
        )
      )
        return yield* invalid("Required common verification checks are missing or did not pass.");
      const projects = yield* sql<{
        cwd: string;
      }>`SELECT workspace_root AS cwd FROM projection_projects WHERE project_id=${state.projectId} AND deleted_at IS NULL`;
      const currentGithub = yield* github.get(state.projectId);
      if (
        !projects[0] ||
        Option.isNone(currentGithub) ||
        !currentGithub.value.config ||
        epicDigest(currentGithub.value.config.repository) !== epicDigest(state.source.repository)
      )
        return yield* epicError(
          "handoff-repository-changed",
          "The project or its GitHub repository binding changed after this Epic ran.",
        );
      const accepted = state.members.filter((member) => member.status === "accepted");
      if (
        !accepted.length ||
        state.members.some(
          (member) => member.status !== "accepted" && member.status !== "external-closed",
        )
      )
        return yield* invalid("The Epic contains work without accepted completion evidence.");
      const reservations: Array<{
        member: AgentControlEpicMemberView;
        original: AgentControlWorktreeReservationState;
        integration: { expectedCommitSha: string; worktreePath: string } | undefined;
      }> = [];
      let childCheckCount = 0;
      for (const member of accepted) {
        if (
          !member.reservationId ||
          !member.childRunId ||
          !member.taskId ||
          !member.taskFinalizationEvidenceId ||
          !member.accepted
        )
          return yield* invalid("An accepted child is missing its durable result authority.");
        const reservation = yield* loadAuthoritativeWorktreeReservation(
          AgentControlWorktreeReservationId.make(member.reservationId),
          events,
          states,
        );
        if (Option.isNone(reservation))
          return yield* invalid("The original worktree reservation evidence is unavailable.");
        const original = reservation.value.statesByVersion[0]!;
        if (
          original.projectId !== state.projectId ||
          original.taskId !== member.taskId ||
          original.repository.repositoryNodeId !== state.source.repository.repositoryNodeId ||
          original.repository.nameWithOwner !== state.source.repository.nameWithOwner ||
          original.repositoryWorkspace !== projects[0].cwd
        )
          return yield* epicError(
            "handoff-repository-changed",
            "The project workspace or original repository mapping changed.",
          );
        const rows = yield* sql<{
          inputJson: string;
          commitSha: string;
          treeSha: string;
          codeDigest: string;
          manifestDigest: string;
          resultJson: string;
          resultDigest: string;
          deliveryId: string;
          providerTurnId: string;
          handoffId: string;
          fenceToken: number;
        }>`
        SELECT intent.input_json AS "inputJson",intent.commit_sha AS "commitSha",intent.tree_sha AS "treeSha",intent.code_digest AS "codeDigest",intent.manifest_digest AS "manifestDigest",result.result_json AS "resultJson",result.result_digest AS "resultDigest",stage.provider_delivery_id AS "deliveryId",stage.provider_turn_id AS "providerTurnId",stage.handoff_id AS "handoffId",stage.fence_token AS "fenceToken"
        FROM agent_control_epic_capture_intents intent
        JOIN agent_control_epic_capture_results result ON result.child_run_id=intent.child_run_id
        JOIN agent_control_task_verification_finalization_evidence task ON task.task_finalization_evidence_id=${member.taskFinalizationEvidenceId} AND task.project_id=${state.projectId} AND task.task_id=${member.taskId} AND task.verification_outcome='succeeded'
        JOIN agent_control_verification_finalization_evidence stage ON stage.finalization_evidence_id=task.verification_evidence_id
        WHERE intent.child_run_id=${member.childRunId} AND intent.epic_run_id=${state.epicRunId} AND intent.project_id=${state.projectId}`;
        const row = rows[0];
        const captured = member.captured ?? member.accepted;
        if (
          rows.length !== 1 ||
          !row ||
          row.resultDigest !== sha256Utf8(row.resultJson) ||
          epicDigest(yield* decodeAccepted(row.resultJson)) !== epicDigest(captured) ||
          row.commitSha !== captured.commitSha ||
          row.treeSha !== captured.treeSha ||
          row.codeDigest !== captured.codeDigest ||
          captured.evidenceId !== `epic-capture:${member.childRunId}` ||
          !row.codeDigest.startsWith(VERIFICATION_CODE_SNAPSHOT_PREFIX) ||
          row.inputJson !==
            epicJson({
              epicRunId: state.epicRunId,
              projectId: state.projectId,
              taskId: member.taskId,
              childRunId: member.childRunId,
              reservationId: member.reservationId,
              taskFinalizationEvidenceId: member.taskFinalizationEvidenceId,
              previousCommitSha: member.baseCommitSha,
            })
        )
          return yield* invalid(
            "An accepted child commit no longer matches its retained capture evidence.",
          );
        const assessment = yield* assessVerificationChecks(
          sql,
          {
            evidence: {
              providerDeliveryId: row.deliveryId,
              handoffId: row.handoffId,
              fenceToken: row.fenceToken,
              worktreePath: original.internalWorktreePath,
            },
            delivery: { providerTurnId: row.providerTurnId },
          },
          { checkCurrentCode: false },
        );
        const seals = yield* sql<{
          digest: string;
          code: string | null;
        }>`SELECT digest,code FROM agent_control_verification_check_assessments WHERE provider_delivery_id=${row.deliveryId}`;
        if (
          row.manifestDigest !== assessment.digest ||
          assessment.code !== null ||
          seals.length !== 1 ||
          seals[0]!.code !== null ||
          seals[0]!.digest !== assessment.digest
        )
          return yield* invalid(
            "Child verification receipts no longer match their accepted assessment.",
          );
        const childManifest = yield* sql<{
          checksJson: string;
          codeDigest: string;
        }>`SELECT checks_json AS "checksJson",code_digest AS "codeDigest" FROM agent_control_verification_check_manifests WHERE provider_delivery_id=${row.deliveryId}`;
        if (
          childManifest.length !== 1 ||
          rawVerificationCodeDigest(childManifest[0]!.codeDigest) !== row.codeDigest
        )
          return yield* invalid(
            "The accepted capture no longer matches its child verification code digest.",
          );
        const childChecks = yield* decodeChecks(childManifest[0]!.checksJson);
        childCheckCount += childChecks.filter((check) => check.required).length;
        let integration: { expectedCommitSha: string; worktreePath: string } | undefined;
        if (member.captured) {
          const integrations = yield* sql<{
            integrationId: string;
            inputJson: string;
            expectedCommitSha: string;
            capturedCommitSha: string;
            commitSha: string;
            treeSha: string;
            worktreePath: string;
            resultJson: string;
            resultDigest: string;
          }>`SELECT intent.integration_id AS "integrationId",intent.input_json AS "inputJson",intent.expected_commit_sha AS "expectedCommitSha",intent.captured_commit_sha AS "capturedCommitSha",intent.commit_sha AS "commitSha",intent.tree_sha AS "treeSha",intent.worktree_path AS "worktreePath",result.result_json AS "resultJson",result.result_digest AS "resultDigest"
            FROM agent_control_epic_integration_intents intent JOIN agent_control_epic_integration_results result ON result.integration_id=intent.integration_id
            WHERE intent.epic_run_id=${state.epicRunId} AND intent.child_run_id=${member.childRunId} AND intent.commit_sha=${member.accepted.commitSha}`;
          const item = integrations[0];
          if (
            !item ||
            integrations.length !== 1 ||
            sha256Utf8(item.resultJson) !== item.resultDigest
          )
            return yield* invalid("An accepted integration receipt is missing or corrupt.");
          const integrated = yield* decodeIntegrated(item.resultJson);
          if (
            epicDigest(integrated.accepted) !== epicDigest(member.accepted) ||
            !member.integrationVerification ||
            epicDigest(integrated.verification) !== epicDigest(member.integrationVerification) ||
            integrated.verification.status !== "passed" ||
            integrated.verification.commitSha !== item.commitSha ||
            integrated.accepted.commitSha !== item.commitSha ||
            integrated.accepted.treeSha !== item.treeSha ||
            item.capturedCommitSha !== captured.commitSha ||
            item.integrationId !== `epic-integration:${sha256Utf8(item.inputJson)}` ||
            integrated.accepted.evidenceId !== item.integrationId ||
            !integrated.verification.evidenceId.startsWith(`${item.integrationId}:`) ||
            item.inputJson !==
              epicJson({
                epicRunId: state.epicRunId,
                projectId: state.projectId,
                childRunId: member.childRunId,
                captured,
                expectedCommitSha: item.expectedCommitSha,
                initialBaseCommitSha: state.initialBase?.commitSha,
                checks: state.checks,
              })
          )
            return yield* invalid(
              "The integration proof does not match the captured task and accepted common result.",
            );
          const manifests = yield* sql<{
            fenceToken: number;
            codeDigest: string;
            checksJson: string;
            manifestDigest: string;
          }>`SELECT fence_token AS "fenceToken",code_digest AS "codeDigest",checks_json AS "checksJson",manifest_digest AS "manifestDigest" FROM agent_control_verification_check_manifests WHERE provider_delivery_id=${integrated.verification.evidenceId}`;
          const manifest = manifests[0];
          if (
            !manifest ||
            manifests.length !== 1 ||
            manifest.manifestDigest !== integrated.verification.manifestDigest ||
            rawVerificationCodeDigest(manifest.codeDigest) !== integrated.accepted.codeDigest ||
            manifest.checksJson !== epicJson(state.checks) ||
            integrated.verification.evidenceId !== `${item.integrationId}:${manifest.fenceToken}`
          )
            return yield* invalid("The integration checks do not bind the current common tree.");
          const assessment = yield* assessVerificationChecks(
            sql,
            {
              evidence: {
                providerDeliveryId: integrated.verification.evidenceId,
                handoffId: integrated.verification.evidenceId,
                fenceToken: manifest.fenceToken,
                worktreePath: item.worktreePath,
              },
              delivery: { providerTurnId: integrated.verification.evidenceId },
            },
            { checkCurrentCode: false },
          );
          if (assessment.code !== null)
            return yield* invalid("Required integration checks are incomplete or invalid.");
          integration = {
            expectedCommitSha: item.expectedCommitSha,
            worktreePath: item.worktreePath,
          };
        }
        reservations.push({ member, original, integration });
      }
      // Follow the accepted commit chain, independent of GitHub issue display order.
      const initialBase = state.initialBase?.commitSha ?? null;
      const previousCommit = (entry: (typeof reservations)[number]) =>
        entry.integration?.expectedCommitSha ?? entry.member.baseCommitSha;
      const first = reservations.find((entry) => previousCommit(entry) === initialBase);
      if (!first) return yield* invalid("The original Epic base could not be determined.");
      let previous: string | null = initialBase;
      const visited = new Set<string>();
      while (visited.size < reservations.length) {
        const next = reservations.filter((entry) => previousCommit(entry) === previous);
        if (next.length !== 1 || visited.has(next[0]!.member.childRunId!))
          return yield* invalid("The accepted child commit chain is inconsistent.");
        const entry = next[0]!;
        if (
          entry.original.baseCommitSha !==
            (entry.integration
              ? entry.member.baseCommitSha
              : (previous ?? first.original.baseCommitSha)) ||
          epicDigest(entry.original.repository) !== epicDigest(first.original.repository) ||
          entry.original.repositoryCommonDir !== first.original.repositoryCommonDir
        )
          return yield* invalid(
            "The accepted children do not share the original repository and base chain.",
          );
        visited.add(entry.member.childRunId!);
        previous = entry.member.accepted!.commitSha;
      }
      if (previous !== state.acceptedCommitSha)
        return yield* invalid("The final commit is not the end of the accepted child chain.");
      const last = reservations.find(
        ({ member }) => member.accepted!.commitSha === state.acceptedCommitSha,
      )!;
      // Capture snapshots include the preceding HEAD and dirty diff; final snapshots
      // include the accepted clean HEAD. The durable final history binds this attempt
      // to the accepted commit; its own manifest and receipts bind the executed checks.
      const manifests = yield* sql<{
        codeDigest: string;
        checksJson: string;
        manifestDigest: string;
      }>`SELECT manifest_digest AS "manifestDigest", code_digest AS "codeDigest", checks_json AS "checksJson" FROM agent_control_verification_check_manifests WHERE provider_delivery_id=${final.evidenceId}`;
      if (
        manifests.length !== 1 ||
        !rawVerificationCodeDigest(manifests[0]!.codeDigest).startsWith(
          VERIFICATION_CODE_SNAPSHOT_PREFIX,
        ) ||
        (final.manifestDigest !== undefined &&
          manifests[0]!.manifestDigest !== final.manifestDigest) ||
        manifests[0]!.checksJson !== epicJson(state.checks)
      )
        return yield* invalid(
          "The common verification manifest is not bound to the accepted commit and configured checks.",
        );
      const assessment = yield* assessVerificationChecks(
        sql,
        {
          evidence: {
            providerDeliveryId: final.evidenceId,
            handoffId: final.evidenceId,
            fenceToken: state.verificationAttempt,
            worktreePath: last.integration?.worktreePath ?? last.original.internalWorktreePath,
          },
          delivery: { providerTurnId: final.evidenceId },
        },
        { checkCurrentCode: false },
      );
      if (assessment.code !== null)
        return yield* invalid(
          "The common verification receipts are missing, corrupt, or unsuccessful.",
        );
      const prefix = `refs/remotes/${first.original.repository.remoteName}/`;
      if (!first.original.repository.defaultRemoteRef.startsWith(prefix))
        return yield* invalid("The original target branch is unavailable.");
      const targetBranch = first.original.repository.defaultRemoteRef.slice(prefix.length);
      if (
        state.initialBase
          ? first.original.baseRef !== state.initialBase.commitSha ||
            first.original.baseCommitSha !== state.initialBase.commitSha ||
            targetBranch !== state.initialBase.targetBranch
          : first.original.baseRef !== `${first.original.repository.remoteName}/${targetBranch}`
      )
        return yield* invalid("The first child was not based on the frozen target branch.");
      return {
        authority: {
          cwd: projects[0].cwd,
          repositoryCommonDir: first.original.repositoryCommonDir,
          repository: first.original.repository,
          targetBranch,
          baseCommitSha: first.original.baseCommitSha,
          commitSha: state.acceptedCommitSha,
        },
        childCheckCount,
        finalCheckCount: required.length,
      };
    },
    Effect.mapError((cause) =>
      isEpicError(cause) ? cause : invalid("The retained Epic authority could not be verified."),
    ),
  );
  const verify: (
    state: AgentControlEpicRuntimeView,
  ) => Effect.Effect<EpicHandoffProof, AgentControlEpicRpcError> = Effect.fn(
    "EpicHandoffEvidence.verify",
  )(
    function* (state: AgentControlEpicRuntimeView) {
      const rework = state.reviewReworks?.findLast((item) => item.status === "succeeded");
      if (!rework) return yield* verifyOriginal(state);
      const final = state.finalVerification;
      if (
        state.status !== "succeeded" ||
        state.activeReviewReworkId != null ||
        !state.acceptedCommitSha ||
        state.acceptedCommitSha !== rework.candidateCommitSha ||
        !final ||
        final.status !== "passed" ||
        final.commitSha !== state.acceptedCommitSha ||
        final.evidenceId !== `epic-review-final:${rework.requestId}` ||
        !rework.verification ||
        epicDigest(rework.verification) !== epicDigest(final) ||
        !state.finalVerificationHistory.some((item) => epicDigest(item) === epicDigest(final))
      )
        return yield* invalid(
          "The repaired Epic has no complete successful review verification authority.",
        );
      const previousFinal = state.finalVerificationHistory.find(
        (item) => item.evidenceId === rework.previousVerificationEvidenceId,
      );
      if (
        !previousFinal ||
        previousFinal.status !== "passed" ||
        previousFinal.commitSha !== rework.previousAcceptedCommitSha ||
        rework.reviewedCommitSha !== rework.previousAcceptedCommitSha ||
        rework.reviewedVerificationEvidenceId !== previousFinal.evidenceId
      )
        return yield* invalid("The reviewed predecessor evidence is missing or inconsistent.");
      const requests = yield* sql<{
        requestJson: string;
        requestDigest: string;
        reviewedCommitSha: string;
        reviewedVerificationEvidenceId: string;
        idempotencyKey: string;
        acceptedRevision: number;
      }>`SELECT request_json AS "requestJson",request_digest AS "requestDigest",
      reviewed_commit_sha AS "reviewedCommitSha",
      reviewed_verification_evidence_id AS "reviewedVerificationEvidenceId",
      idempotency_key AS "idempotencyKey",accepted_revision AS "acceptedRevision"
      FROM main.agent_control_epic_review_requests
      WHERE request_id=${rework.requestId} AND project_id=${state.projectId}
        AND epic_run_id=${state.epicRunId}`;
      if (
        requests.length !== 1 ||
        requests[0]!.requestDigest !== sha256Utf8(requests[0]!.requestJson) ||
        requests[0]!.requestJson !==
          epicJson({
            projectId: state.projectId,
            epicRunId: state.epicRunId,
            expectedRevision: requests[0]!.acceptedRevision - 1,
            reviewedCommitSha: rework.reviewedCommitSha,
            reviewedVerificationEvidenceId: rework.reviewedVerificationEvidenceId,
            findings: rework.findings,
            idempotencyKey: rework.idempotencyKey,
          }) ||
        requests[0]!.idempotencyKey !== rework.idempotencyKey ||
        requests[0]!.reviewedCommitSha !== rework.reviewedCommitSha ||
        requests[0]!.reviewedVerificationEvidenceId !== rework.reviewedVerificationEvidenceId
      )
        return yield* invalid("The immutable review request evidence is missing or corrupt.");
      const acceptedHistory = yield* sql<{
        stateJson: string;
        stateDigest: string;
        activeRequestId: string | null;
      }>`SELECT state_json AS "stateJson",state_digest AS "stateDigest",
      json_extract(state_json,'$.activeReviewReworkId') AS "activeRequestId"
      FROM main.agent_control_epic_history
      WHERE epic_run_id=${state.epicRunId} AND revision=${requests[0]!.acceptedRevision}`;
      if (
        acceptedHistory.length !== 1 ||
        acceptedHistory[0]!.stateDigest !== sha256Utf8(acceptedHistory[0]!.stateJson) ||
        acceptedHistory[0]!.activeRequestId !== rework.requestId
      )
        return yield* invalid("The accepted review request history is missing or corrupt.");
      const repairResults = yield* sql<{
        attempt: number;
        intentJson: string;
        intentDigest: string;
        providerInstanceId: string;
        model: string;
        threadId: string;
        turnRequestCommandId: string;
        messageId: string;
        claimedAt: string | null;
        receiptTurnId: string | null;
        projectedTurnId: string | null;
        pendingMessageId: string | null;
        turnState: string | null;
        checkpointRef: string | null;
        checkpointStatus: string | null;
        resultJson: string;
        resultDigest: string;
      }>`SELECT result.attempt,intent.intent_json AS "intentJson",
      intent.intent_digest AS "intentDigest",
      intent.provider_instance_id AS "providerInstanceId",intent.model,
      intent.thread_id AS "threadId",intent.turn_request_command_id AS "turnRequestCommandId",
      intent.message_id AS "messageId",claim.claimed_at AS "claimedAt",
      receipt.provider_turn_id AS "receiptTurnId",turn.turn_id AS "projectedTurnId",
      turn.pending_message_id AS "pendingMessageId",turn.state AS "turnState",
      turn.checkpoint_ref AS "checkpointRef",turn.checkpoint_status AS "checkpointStatus",
      COALESCE(recovery.result_json,result.result_json) AS "resultJson",
      COALESCE(recovery.result_digest,result.result_digest) AS "resultDigest"
      FROM main.agent_control_epic_review_repair_results result
      JOIN main.agent_control_epic_review_repair_intents intent
        ON intent.request_id=result.request_id AND intent.attempt=result.attempt
      LEFT JOIN main.agent_control_epic_review_repair_delivery_claims claim
        ON claim.request_id=intent.request_id AND claim.attempt=intent.attempt
      LEFT JOIN main.agent_control_epic_review_repair_delivery_receipts receipt
        ON receipt.request_id=intent.request_id AND receipt.attempt=intent.attempt
      LEFT JOIN main.projection_turns turn
        ON turn.thread_id=intent.thread_id AND turn.pending_message_id=intent.message_id
      LEFT JOIN main.agent_control_epic_review_repair_recoveries recovery
        ON recovery.request_id=result.request_id AND recovery.attempt=result.attempt
          AND recovery.original_result_digest=result.result_digest
      WHERE result.request_id=${rework.requestId}
      ORDER BY result.attempt DESC LIMIT 1`;
      if (
        repairResults.length !== 1 ||
        repairResults[0]!.intentDigest !== sha256Utf8(repairResults[0]!.intentJson) ||
        repairResults[0]!.resultDigest !== sha256Utf8(repairResults[0]!.resultJson) ||
        repairResults[0]!.turnRequestCommandId !==
          `epic-review-turn:${rework.requestId}:${repairResults[0]!.attempt}` ||
        repairResults[0]!.messageId !==
          `epic-review-message:${rework.requestId}:${repairResults[0]!.attempt}` ||
        !repairResults[0]!.claimedAt ||
        !repairResults[0]!.receiptTurnId ||
        repairResults[0]!.receiptTurnId !== repairResults[0]!.projectedTurnId ||
        repairResults[0]!.pendingMessageId !== repairResults[0]!.messageId ||
        repairResults[0]!.turnState !== "completed" ||
        !repairResults[0]!.checkpointRef ||
        repairResults[0]!.checkpointStatus !== "ready"
      )
        return yield* invalid(
          "The accepted repair result is not bound to its authorized provider turn and checkpoint.",
        );
      const repairAttempt = rework.repairAttempts.find(
        (attempt) => attempt.attempt === repairResults[0]!.attempt,
      );
      if (
        !repairAttempt ||
        repairAttempt.status !== "succeeded" ||
        repairAttempt.providerInstanceId !== repairResults[0]!.providerInstanceId ||
        repairAttempt.model !== repairResults[0]!.model ||
        repairAttempt.threadId !== repairResults[0]!.threadId
      )
        return yield* invalid("The accepted repair attempt does not match its immutable intent.");
      const repairResult = yield* decodeReviewRepairResult(repairResults[0]!.resultJson);
      if (
        repairResult.status !== "succeeded" ||
        repairResult.candidateCommitSha !== state.acceptedCommitSha
      )
        return yield* invalid("The accepted repair result does not match the current commit.");
      const required = state.checks.filter((check) => check.required);
      if (
        !required.length ||
        required.some(
          (check) =>
            !final.checks.some(
              (result) =>
                result.id === check.id && result.status === "passed" && result.exitCode === 0,
            ),
        )
      )
        return yield* invalid("Required repaired-result checks are missing or did not pass.");
      const manifests = yield* sql<{
        fenceToken: number;
        worktreePath: string;
        codeDigest: string;
        checksJson: string;
        manifestDigest: string;
      }>`SELECT fence_token AS "fenceToken",worktree_path AS "worktreePath",
      code_digest AS "codeDigest",checks_json AS "checksJson",
      manifest_digest AS "manifestDigest"
      FROM main.agent_control_verification_check_manifests
      WHERE provider_delivery_id=${final.evidenceId}`;
      const manifest = manifests[0];
      if (
        !manifest ||
        manifests.length !== 1 ||
        manifest.fenceToken !== state.verificationAttempt ||
        manifest.checksJson !== epicJson(state.checks) ||
        manifest.manifestDigest !== final.manifestDigest ||
        !rawVerificationCodeDigest(manifest.codeDigest).startsWith(
          VERIFICATION_CODE_SNAPSHOT_PREFIX,
        )
      )
        return yield* invalid("The repaired-result verification manifest is invalid.");
      const assessment = yield* assessVerificationChecks(
        sql,
        {
          evidence: {
            providerDeliveryId: final.evidenceId,
            handoffId: final.evidenceId,
            fenceToken: state.verificationAttempt,
            worktreePath: manifest.worktreePath,
          },
          delivery: { providerTurnId: final.evidenceId },
        },
        { checkCurrentCode: false },
      );
      if (assessment.code !== null)
        return yield* invalid(
          "The repaired-result verification receipts are incomplete or unsuccessful.",
        );
      const previousAttempt = Math.max(1, state.verificationAttempt - 1);
      const reworkIndex = state.reviewReworks?.findLastIndex(
        (item) => item.requestId === rework.requestId,
      );
      const previousProof = yield* verify({
        ...state,
        status: "succeeded",
        acceptedCommitSha: rework.previousAcceptedCommitSha,
        verificationAttempt: previousAttempt,
        finalVerification: previousFinal,
        activeReviewReworkId: null,
        reviewReworks:
          reworkIndex === undefined || reworkIndex < 0
            ? []
            : (state.reviewReworks ?? []).slice(0, reworkIndex),
      });
      return {
        ...previousProof,
        authority: { ...previousProof.authority, commitSha: state.acceptedCommitSha },
        finalCheckCount: required.length,
      };
    },
    Effect.mapError((cause) =>
      isEpicError(cause)
        ? cause
        : invalid("The retained review repair authority could not be verified."),
    ),
  );
  return { verify };
});
