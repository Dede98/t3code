// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import {
  AgentControlEpicRpcError,
  CommandId,
  type AgentControlEpicHandoff,
  type AgentControlEpicHandoffPreview,
  type AgentControlEpicHandoffPreviewInput,
  type AgentControlEpicHandoffPublishInput,
  type AgentControlEpicRuntimeView,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { epicDigest, epicError, loadEpicRun, saveEpicRun } from "./authority.ts";
import { EpicHandoffEvidence } from "./handoffAuthority.ts";
import { EpicHandoffRemote, EpicHandoffRemoteError } from "./remote.ts";

const isEpicError = Schema.is(AgentControlEpicRpcError);
const isRemoteError = Schema.is(EpicHandoffRemoteError);
const now = Effect.map(DateTime.now, DateTime.formatIso);
const mapError = (cause: unknown) =>
  isEpicError(cause)
    ? cause
    : epicError(
        "handoff-unavailable",
        "The handoff could not be completed. Retry to reconcile its saved remote state.",
      );
const branchFor = (state: AgentControlEpicRuntimeView, intentId: string) =>
  `t3auto/epic-${state.source.epic.number}-${intentId}`;

export const makeEpicHandoff = Effect.fn("makeEpicHandoff")(function* (options: {
  readonly onChange: (projectId: ProjectId) => Effect.Effect<void>;
  readonly withProjectLock: <A, E, R>(
    projectId: ProjectId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}) {
  const sql = yield* SqlClient.SqlClient;
  const evidence = yield* EpicHandoffEvidence;
  const remote = yield* EpicHandoffRemote;
  const load = Effect.fn("EpicHandoff.load")(function* (
    input: AgentControlEpicHandoffPreviewInput,
  ) {
    const state = yield* loadEpicRun(sql, input.epicRunId);
    if (!state || state.projectId !== input.projectId)
      return yield* epicError(
        "epic-unavailable",
        "This Epic run does not belong to the selected project and environment.",
      );
    return state;
  });
  const persist = (state: AgentControlEpicRuntimeView, handoff: AgentControlEpicHandoff) =>
    sql
      .withTransaction(saveEpicRun(sql, state, { handoff }))
      .pipe(Effect.tap(() => options.onChange(state.projectId)));

  const previewHandoff = Effect.fn("EpicHandoff.preview")(function* (
    input: AgentControlEpicHandoffPreviewInput,
  ): Effect.fn.Return<AgentControlEpicHandoffPreview, AgentControlEpicRpcError> {
    const state = yield* load(input).pipe(Effect.mapError(mapError));
    const base = {
      projectId: state.projectId,
      epicRunId: state.epicRunId,
      repository: state.source.repository,
      commitSha: state.acceptedCommitSha,
      branchName: state.handoff?.branchName ?? null,
      targetBranch: state.handoff?.targetBranch ?? null,
      handoff: state.handoff ?? null,
    };
    if (
      state.handoff?.pullRequest ||
      (state.handoff?.status === "blocked" &&
        ["remote-branch-collision", "pull-request-collision"].includes(
          state.handoff.error?.code ?? "",
        ))
    )
      return {
        ...base,
        canPublish: false,
        blockers: state.handoff.error ? [{ ...state.handoff.error, issueNumber: null }] : [],
      };
    const checked = yield* Effect.result(
      Effect.gen(function* () {
        const proof = yield* evidence.verify(state);
        yield* remote.prepare(
          {
            ...proof.authority,
            branchName: base.branchName ?? "t3auto/preview",
            ownershipToken: state.handoff?.intentId ?? "preview-readonly-0000",
            branchCreationAttempted: state.handoff?.branchCreationAttempted ?? false,
          },
          { initialPreview: state.handoff === undefined },
        );
        return proof;
      }),
    );
    if (checked._tag === "Failure") {
      const error = checked.failure;
      return {
        ...base,
        canPublish: false,
        blockers: [{ code: error.code, message: error.message, issueNumber: null }],
      };
    }
    return {
      ...base,
      targetBranch: checked.success.authority.targetBranch,
      canPublish: state.handoff?.status !== "publishing",
      blockers: [],
    };
  });

  const publish = Effect.fn("EpicHandoff.publishLocked")(function* (
    input: AgentControlEpicHandoffPublishInput,
  ) {
    // Keep a received remote result and its durable outcome together. Network work
    // remains interruptible; a known collision must never become an ambiguous retry.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let state = yield* load(input);
        if (
          state.acceptedCommitSha !== input.expectedCommitSha ||
          (state.handoff &&
            (state.handoff.commitSha !== input.expectedCommitSha ||
              state.handoff.targetBranch !== input.expectedTargetBranch))
        )
          return yield* epicError(
            "handoff-confirmation-stale",
            "The confirmed commit or target branch no longer matches this Epic handoff.",
          );
        if (
          state.handoff?.pullRequest ||
          (state.handoff?.status === "blocked" &&
            ["remote-branch-collision", "pull-request-collision"].includes(
              state.handoff.error?.code ?? "",
            ))
        )
          return state;
        if (!state.handoff && state.revision !== input.expectedRevision)
          return yield* epicError(
            "revision-conflict",
            "Epic progress changed; review the publication details again.",
          );
        let handoff = state.handoff;
        const result = yield* Effect.result(
          Effect.gen(function* () {
            const proof = yield* restore(evidence.verify(state));
            if (
              proof.authority.targetBranch !== input.expectedTargetBranch ||
              proof.authority.commitSha !== input.expectedCommitSha
            )
              return yield* epicError(
                "handoff-confirmation-stale",
                "The confirmed commit or target branch changed. Review the publication details again.",
              );
            if (
              handoff &&
              (handoff.baseCommitSha !== proof.authority.baseCommitSha ||
                epicDigest(handoff.repository) !== epicDigest(state.source.repository) ||
                handoff.verificationEvidenceId !== state.finalVerification!.evidenceId)
            )
              return yield* epicError(
                "handoff-authority-changed",
                "The saved publication intent no longer matches the accepted Epic authority.",
              );
            const timestamp = yield* now;
            const intentId = handoff?.intentId ?? NodeCrypto.randomUUID();
            handoff = handoff
              ? { ...handoff, status: "publishing" as const, error: null, updatedAt: timestamp }
              : {
                  intentId,
                  status: "publishing" as const,
                  repository: state.source.repository,
                  targetBranch: proof.authority.targetBranch,
                  baseCommitSha: proof.authority.baseCommitSha,
                  commitSha: proof.authority.commitSha,
                  branchName: branchFor(state, intentId),
                  verificationEvidenceId: state.finalVerification!.evidenceId,
                  branchCreationAttempted: false,
                  requestedAt: timestamp,
                  updatedAt: timestamp,
                  pullRequest: null,
                  error: null,
                };
            // Intent commits before any remote mutation. Every retry uses this identity.
            state = yield* persist(state, handoff);
            const authority = {
              ...proof.authority,
              branchName: handoff.branchName,
              ownershipToken: handoff.intentId,
            };
            const pullRequest = yield* restore(
              remote.publish(
                {
                  ...authority,
                  branchCreationAttempted: handoff.branchCreationAttempted ?? false,
                  epicNumber: state.source.epic.number,
                  childIssueNumbers: state.members
                    .filter((member) => member.status === "accepted")
                    .map((member) => member.issueNumber),
                  childCheckCount: proof.childCheckCount,
                  finalCheckCount: proof.finalCheckCount,
                },
                {
                  beforeBranchCreate: () =>
                    Effect.gen(function* () {
                      handoff = {
                        ...handoff!,
                        branchCreationAttempted: true,
                        updatedAt: yield* now,
                      };
                      state = yield* persist(state, handoff);
                    }).pipe(
                      Effect.mapError(
                        () =>
                          new EpicHandoffRemoteError({
                            code: "handoff-persistence-failed",
                            message: "The branch creation intent could not be saved.",
                          }),
                      ),
                    ),
                },
              ),
            );
            const blocked = pullRequest.state !== "open" || !pullRequest.isDraft;
            handoff = {
              ...handoff!,
              status: blocked ? "blocked" : "published",
              pullRequest,
              updatedAt: yield* now,
              error: blocked
                ? {
                    code: "handoff-pr-unavailable",
                    message:
                      pullRequest.state === "merged"
                        ? "The existing pull request was merged. This handoff will not create a replacement."
                        : pullRequest.state === "closed"
                          ? "The existing pull request was closed. Reopen it on GitHub to continue review."
                          : "The existing pull request is no longer a draft. Open it on GitHub to continue review.",
                  }
                : null,
            };
            state = yield* persist(state, handoff);
            return state;
          }),
        );
        if (result._tag === "Success") return result.success;
        const cause = result.failure;
        const failure =
          isEpicError(cause) || isRemoteError(cause)
            ? { code: cause.code, message: cause.message }
            : {
                code: "handoff-unavailable",
                message:
                  "The handoff could not be completed. Retry to reconcile its saved remote state.",
              };
        if (!handoff) return yield* epicError(failure.code, failure.message);
        // Preserve the local successful result even if push/PR response or persistence was lost.
        const current = yield* load(input);
        if (current.handoff?.pullRequest) return current;
        return yield* persist(current, {
          ...handoff,
          status: [
            "remote-unavailable",
            "remote-branch-rejected",
            "remote-response-invalid",
            "handoff-unavailable",
            "handoff-persistence-failed",
          ].includes(failure.code)
            ? "failed"
            : "blocked",
          // A known rejection cannot authorize adopting a branch that appears later.
          branchCreationAttempted:
            failure.code === "remote-branch-rejected" ||
            failure.code === "handoff-persistence-failed"
              ? false
              : (handoff.branchCreationAttempted ?? false),
          updatedAt: yield* now,
          error: failure,
        });
      }),
    );
  }, Effect.mapError(mapError));

  const publishHandoff = (input: AgentControlEpicHandoffPublishInput) =>
    options.withProjectLock(
      input.projectId,
      publish(input).pipe(
        Effect.onInterrupt(() =>
          Effect.gen(function* () {
            const state = yield* load(input);
            if (state.handoff?.status !== "publishing") return;
            yield* persist(state, {
              ...state.handoff,
              status: "failed",
              updatedAt: yield* now,
              error: {
                code: "handoff-interrupted",
                message:
                  "The connection ended before the handoff was confirmed. Retry to reconcile the same branch and pull request.",
              },
            });
          }).pipe(Effect.catch(() => Effect.void)),
        ),
      ),
    );
  const recoverPending = Effect.fn("EpicHandoff.recoverPending")(function* () {
    const rows = yield* sql<{
      epicRunId: string;
      projectId: ProjectId;
    }>`SELECT epic_run_id AS "epicRunId",project_id AS "projectId" FROM agent_control_epic_runs WHERE json_extract(state_json,'$.handoff.status')='publishing'`;
    for (const row of rows) {
      const state = yield* load(row);
      if (!state.handoff) continue;
      yield* publishHandoff({
        ...row,
        commandId: CommandId.make(`epic-handoff-recovery:${state.handoff.intentId}`),
        expectedRevision: state.revision,
        expectedCommitSha: state.handoff.commitSha,
        expectedTargetBranch: state.handoff.targetBranch,
      }).pipe(Effect.catch(() => Effect.void));
    }
  }, Effect.mapError(mapError));
  return { previewHandoff, publishHandoff, recoverPending };
});
