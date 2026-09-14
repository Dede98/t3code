import {
  AgentControlEpicAcceptedResult,
  AgentControlVerificationChecks,
  AgentControlWorktreeReservationId,
  type AgentControlEpicRuntimeView,
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
  const verify = Effect.fn("EpicHandoffEvidence.verify")(
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
      const reservations = [];
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
        if (
          rows.length !== 1 ||
          !row ||
          row.resultDigest !== sha256Utf8(row.resultJson) ||
          epicDigest(yield* decodeAccepted(row.resultJson)) !== epicDigest(member.accepted) ||
          row.commitSha !== member.accepted.commitSha ||
          row.treeSha !== member.accepted.treeSha ||
          row.codeDigest !== member.accepted.codeDigest ||
          member.accepted.evidenceId !== `epic-capture:${member.childRunId}` ||
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
        if (childManifest.length !== 1 || childManifest[0]!.codeDigest !== row.codeDigest)
          return yield* invalid(
            "The accepted capture no longer matches its child verification code digest.",
          );
        const childChecks = yield* decodeChecks(childManifest[0]!.checksJson);
        childCheckCount += childChecks.filter((check) => check.required).length;
        reservations.push({ member, original });
      }
      // Follow the accepted commit chain, independent of GitHub issue display order.
      const first = reservations.find(({ member }) => member.baseCommitSha === null);
      if (!first) return yield* invalid("The original Epic base could not be determined.");
      let previous: string | null = null;
      const visited = new Set<string>();
      while (visited.size < reservations.length) {
        const next = reservations.filter(({ member }) => member.baseCommitSha === previous);
        if (next.length !== 1 || visited.has(next[0]!.member.childRunId!))
          return yield* invalid("The accepted child commit chain is inconsistent.");
        const entry = next[0]!;
        if (
          entry.original.baseCommitSha !== (previous ?? first.original.baseCommitSha) ||
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
        !manifests[0]!.codeDigest.startsWith(VERIFICATION_CODE_SNAPSHOT_PREFIX) ||
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
            worktreePath: last.original.internalWorktreePath,
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
      if (first.original.baseRef !== `${first.original.repository.remoteName}/${targetBranch}`)
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
  return { verify };
});
