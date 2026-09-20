// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import {
  CommandId,
  AgentControlEpicRpcError,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type AgentControlEpicReviewRepairAttempt,
  type ModelSelection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { checkpointRefForThreadTurn } from "../../checkpointing/Utils.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlPolicyService } from "../AgentControlPolicyService.ts";
import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import { epicError } from "./authority.ts";
import {
  AgentControlEpicReviewRepair,
  type AgentControlEpicReviewRepairInput,
} from "./Services/AgentControlEpicReviewRepair.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
const objectId = /^[0-9a-f]{40}(?:[0-9a-f]{24})?$/;
const git = (cwd: string, args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
  Effect.tryPromise({
    try: async () =>
      (
        await execFile("git", [...args], {
          cwd,
          env: { ...process.env, GIT_OPTIONAL_LOCKS: "0", ...env },
          maxBuffer: 32 * 1024 * 1024,
          timeout: 30_000,
          killSignal: "SIGKILL",
        })
      ).stdout.trim(),
    catch: () =>
      epicError(
        "review-repair-git-failed",
        "The retained review repair worktree could not be read or updated safely.",
      ),
  }).pipe(Effect.uninterruptible);

const resultSchema = Schema.Struct({
  status: Schema.Literals(["succeeded", "failed", "blocked"]),
  candidateCommitSha: Schema.NullOr(Schema.String),
  code: Schema.NullOr(Schema.String),
  message: Schema.NullOr(Schema.String),
});
const decodeResult = Schema.decodeUnknownEffect(Schema.fromJsonString(resultSchema));
const isEpicError = Schema.is(AgentControlEpicRpcError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const policy = yield* AgentControlPolicyService;
  const orchestration = yield* OrchestrationEngineService;

  const progress = Effect.fn("AgentControlEpicReviewRepair.progress")(
    function* (input: AgentControlEpicReviewRepairInput, recovering = false) {
      yield* input.authorize;
      const projects = yield* sql<{ cwd: string }>`SELECT workspace_root AS cwd
      FROM main.projection_projects WHERE project_id=${input.state.projectId} AND deleted_at IS NULL`;
      if (!projects[0])
        return yield* epicError(
          "project-unavailable",
          "The selected project is no longer available in this environment.",
        );
      const projectCwd = projects[0].cwd;
      const commonDir = yield* git(projectCwd, [
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
      ]).pipe(Effect.flatMap((path) => Effect.tryPromise(() => NodeFSP.realpath(path))));
      const suffix = sha256Utf8(input.rework.requestId).slice(0, 24);
      const repairLocation = (attempt: number) => {
        const branchName = `t3auto/epic-review-${suffix}-${attempt}`;
        return {
          branchName,
          branchRef: `refs/heads/${branchName}`,
          worktreePath: NodePath.join(commonDir, "t3-epic-review-repairs", `${suffix}-${attempt}`),
        };
      };

      const rows = yield* sql<{
        attempt: number;
        providerInstanceId: string;
        model: string;
        runtimeMode: "full-access" | "approval-required";
        threadId: string;
        turnRequestCommandId: string;
        messageId: string;
        worktreePath: string;
        branchName: string;
        createdAt: string;
        originalResultJson: string | null;
        originalResultDigest: string | null;
        resultJson: string | null;
        completedAt: string | null;
      }>`SELECT intent.attempt,intent.provider_instance_id AS "providerInstanceId",
      intent.model,intent.runtime_mode AS "runtimeMode",intent.thread_id AS "threadId",intent.worktree_path AS "worktreePath",
      intent.turn_request_command_id AS "turnRequestCommandId",intent.message_id AS "messageId",
      intent.branch_name AS "branchName",intent.created_at AS "createdAt",
      result.result_json AS "originalResultJson",result.result_digest AS "originalResultDigest",
      COALESCE(recovery.result_json,result.result_json) AS "resultJson",
      COALESCE(recovery.completed_at,result.completed_at) AS "completedAt"
      FROM main.agent_control_epic_review_repair_intents intent
      LEFT JOIN main.agent_control_epic_review_repair_results result
        ON result.request_id=intent.request_id AND result.attempt=intent.attempt
      LEFT JOIN main.agent_control_epic_review_repair_recoveries recovery
        ON recovery.request_id=result.request_id AND recovery.attempt=result.attempt
          AND recovery.original_result_digest=result.result_digest
      WHERE intent.request_id=${input.rework.requestId} ORDER BY intent.attempt`;
      const recoverable = rows.at(-1);
      if (recovering) {
        const original = recoverable?.originalResultJson
          ? yield* decodeResult(recoverable.originalResultJson)
          : null;
        const requests = yield* sql`SELECT 1 FROM main.agent_control_epic_review_requests
          WHERE request_id=${input.rework.requestId} AND project_id=${input.state.projectId}
            AND epic_run_id=${input.state.epicRunId}
            AND reviewed_commit_sha=${input.rework.reviewedCommitSha}
            AND reviewed_verification_evidence_id=${input.rework.previousVerificationEvidenceId}`;
        const cancelled =
          yield* sql`SELECT 1 FROM main.agent_control_epic_review_repair_cancellations
          WHERE request_id=${input.rework.requestId}`;
        if (
          !recoverable ||
          requests.length !== 1 ||
          cancelled.length !== 0 ||
          original?.status !== "failed" ||
          original.code !== "review-repair-turn-failed" ||
          original.message !== "The repair provider turn did not complete successfully." ||
          recoverable.originalResultDigest !== sha256Utf8(recoverable.originalResultJson!)
        )
          return yield* epicError(
            "review-rework-terminal",
            "This review failure has no recoverable checkpoint evidence.",
          );
      }
      const ensureDispatched = Effect.fn("AgentControlEpicReviewRepair.ensureDispatched")(
        function* (intent: {
          readonly attempt: number;
          readonly providerInstanceId: string;
          readonly model: string;
          readonly runtimeMode: "full-access" | "approval-required";
          readonly threadId: string;
          readonly turnRequestCommandId: string;
          readonly messageId: string;
          readonly createdAt: string;
          readonly worktreePath: string;
          readonly branchName: string;
        }) {
          const { branchName, branchRef, worktreePath } = repairLocation(intent.attempt);
          if (intent.worktreePath !== worktreePath || intent.branchName !== branchName)
            return yield* epicError(
              "authority-conflict",
              "Retained review repair intent points at a different branch or worktree.",
            );
          let head = yield* git(projectCwd, ["rev-parse", "--verify", branchRef]).pipe(
            Effect.catch(() => Effect.succeed(null)),
          );
          if (head === null) {
            yield* input.authorize;
            yield* git(projectCwd, [
              "update-ref",
              branchRef,
              input.rework.reviewedCommitSha,
              "0".repeat(40),
            ]);
            head = input.rework.reviewedCommitSha;
          }
          yield* git(projectCwd, [
            "merge-base",
            "--is-ancestor",
            input.rework.reviewedCommitSha,
            head,
          ]);
          const worktreeExists = yield* Effect.tryPromise(() =>
            NodeFSP.lstat(worktreePath).then(
              () => true,
              (error: NodeJS.ErrnoException) => {
                if (error.code === "ENOENT") return false;
                throw error;
              },
            ),
          );
          if (!worktreeExists) {
            yield* Effect.tryPromise(() =>
              NodeFSP.mkdir(NodePath.dirname(worktreePath), { recursive: true }),
            );
            yield* git(projectCwd, ["worktree", "add", worktreePath, branchName]);
          }
          const deliveryClaim = yield* sql`SELECT 1
          FROM main.agent_control_epic_review_repair_delivery_claims
          WHERE request_id=${input.rework.requestId} AND attempt=${intent.attempt}`;
          if (
            deliveryClaim.length === 0 &&
            (head !== input.rework.reviewedCommitSha ||
              (yield* git(worktreePath, ["status", "--porcelain", "--untracked-files=all"])))
          )
            return yield* epicError(
              "review-repair-worktree-changed",
              "The isolated repair worktree changed before its authorized provider turn began.",
            );
          const selection: ModelSelection = {
            instanceId: ProviderInstanceId.make(intent.providerInstanceId),
            model: intent.model,
          };
          const threadId = ThreadId.make(intent.threadId);
          const createThread = {
            type: "thread.create" as const,
            commandId: CommandId.make(
              `epic-review-thread:${input.rework.requestId}:${intent.attempt}`,
            ),
            threadId,
            projectId: input.state.projectId,
            title: `Epic #${input.state.source.epic.number} review repair`,
            modelSelection: selection,
            runtimeMode: intent.runtimeMode,
            interactionMode: "default" as const,
            branch: branchName,
            worktreePath,
            createdAt: intent.createdAt,
          };
          const prompt = [
            "Repair only the concrete independent-review findings below in the existing verified Epic result.",
            "Do not create a replacement task, broaden scope, publish, push, merge, or change the reviewed base.",
            "Apply the smallest complete fix. Do not reuse old verification as proof; T3 will run required checks after this turn.",
            canonicalJson({
              epicRunId: input.state.epicRunId,
              reviewedCommitSha: input.rework.reviewedCommitSha,
              findings: input.rework.findings,
            }),
          ].join("\n\n");
          yield* input.authorize;
          yield* orchestration
            .dispatch(createThread)
            .pipe(
              Effect.mapError(() =>
                epicError("epic-unavailable", "The repair thread could not be created yet."),
              ),
            );
          yield* input.authorize;
          yield* orchestration
            .dispatch({
              type: "thread.turn.start",
              commandId: CommandId.make(intent.turnRequestCommandId),
              threadId,
              message: {
                messageId: MessageId.make(intent.messageId),
                role: "user",
                text: prompt,
                attachments: [],
              },
              modelSelection: selection,
              runtimeMode: createThread.runtimeMode,
              interactionMode: "default",
              createdAt: intent.createdAt,
            })
            .pipe(
              Effect.mapError(() =>
                epicError("epic-unavailable", "The repair provider turn could not start yet."),
              ),
            );
        },
      );
      const attempts: AgentControlEpicReviewRepairAttempt[] = [];
      for (const row of rows) {
        const location = repairLocation(row.attempt);
        const expectedTurnRequestCommandId = `epic-review-turn:${input.rework.requestId}:${row.attempt}`;
        const expectedMessageId = `epic-review-message:${input.rework.requestId}:${row.attempt}`;
        if (
          row.worktreePath !== location.worktreePath ||
          row.branchName !== location.branchName ||
          row.turnRequestCommandId !== expectedTurnRequestCommandId ||
          row.messageId !== expectedMessageId
        )
          return yield* epicError(
            "authority-conflict",
            "Retained review repair intent points at different execution evidence.",
          );
        const result = row.resultJson ? yield* decodeResult(row.resultJson) : null;
        attempts.push({
          attempt: row.attempt,
          providerInstanceId: row.providerInstanceId,
          model: row.model,
          threadId: row.threadId,
          status: result
            ? result.status === "succeeded"
              ? "succeeded"
              : result.status === "blocked"
                ? "stopped"
                : "failed"
            : "running",
          startedAt: row.createdAt,
          completedAt: row.completedAt,
          error:
            result?.status === "failed" || result?.status === "blocked"
              ? {
                  code: result.code ?? "review-repair-failed",
                  message: result.message ?? "Repair failed.",
                }
              : null,
        });
        if (result?.status === "succeeded" && result.candidateCommitSha)
          return { kind: "candidate" as const, attempts, commitSha: result.candidateCommitSha };
        if (result?.status === "blocked")
          return {
            kind: "blocked" as const,
            attempts,
            code: result.code ?? "review-repair-delivery-ambiguous",
            message:
              result.message ??
              "The repair delivery has an ambiguous provider outcome and cannot be retried safely.",
          };
      }

      const active = recovering ? recoverable : rows.findLast((row) => row.resultJson === null);
      if (active) {
        const activeLocation = repairLocation(active.attempt);
        const observed = yield* sql<{
          sessionStatus: string | null;
          lastError: string | null;
          turnState: string | null;
          turnId: string | null;
          checkpointRef: string | null;
          checkpointStatus: string | null;
          checkpointTurnCount: number | null;
          receiptTurnId: string | null;
          claimedAt: string | null;
        }>`SELECT session.status AS "sessionStatus",session.last_error AS "lastError",
        turn.state AS "turnState",turn.turn_id AS "turnId",turn.checkpoint_ref AS "checkpointRef",
        turn.checkpoint_status AS "checkpointStatus",turn.checkpoint_turn_count AS "checkpointTurnCount",receipt.provider_turn_id AS "receiptTurnId",
        claim.claimed_at AS "claimedAt" FROM main.projection_threads thread
        LEFT JOIN main.projection_thread_sessions session ON session.thread_id=thread.thread_id
        LEFT JOIN main.projection_turns turn ON turn.thread_id=thread.thread_id
          AND turn.pending_message_id=${active.messageId}
        LEFT JOIN main.agent_control_epic_review_repair_delivery_claims claim
          ON claim.request_id=${input.rework.requestId} AND claim.attempt=${active.attempt}
        LEFT JOIN main.agent_control_epic_review_repair_delivery_receipts receipt
          ON receipt.request_id=claim.request_id AND receipt.attempt=claim.attempt
        WHERE thread.thread_id=${active.threadId} AND thread.project_id=${input.state.projectId}
          AND thread.worktree_path=${activeLocation.worktreePath}`;
        const terminal = observed[0];
        if (recovering) {
          // A provider diff is provisional. Recovery requires the later, owned final
          // checkpoint and rejects any durable terminal checkpoint error in between.
          const evidence = yield* sql`SELECT 1 FROM main.orchestration_events placeholder
            JOIN main.orchestration_events final ON final.stream_id=placeholder.stream_id
              AND final.sequence>placeholder.sequence
            WHERE placeholder.stream_id=${active.threadId}
              AND placeholder.event_type='thread.turn-diff-completed'
              AND json_extract(placeholder.payload_json,'$.turnId')=${terminal?.turnId ?? ""}
              AND json_extract(placeholder.payload_json,'$.status')='missing'
              AND json_extract(placeholder.payload_json,'$.checkpointRef') LIKE 'provider-diff:%'
              AND final.event_type='thread.turn-diff-completed'
              AND json_extract(final.payload_json,'$.turnId')=${terminal?.turnId ?? ""}
              AND json_extract(final.payload_json,'$.status')='ready'
              AND json_extract(final.payload_json,'$.checkpointRef')=${terminal?.checkpointRef ?? ""}
              AND NOT EXISTS (SELECT 1 FROM main.orchestration_events failed
                WHERE failed.stream_id=placeholder.stream_id
                  AND failed.event_type='thread.turn-diff-completed'
                  AND json_extract(failed.payload_json,'$.turnId')=${terminal?.turnId ?? ""}
                  AND (json_extract(failed.payload_json,'$.status')='error'
                    OR (json_extract(failed.payload_json,'$.status')='missing'
                      AND json_extract(failed.payload_json,'$.checkpointRef') NOT LIKE 'provider-diff:%')))
              AND NOT EXISTS (SELECT 1 FROM main.orchestration_events failedSession
                WHERE failedSession.stream_id=placeholder.stream_id
                  AND failedSession.event_type='thread.session-set'
                  AND json_extract(failedSession.payload_json,'$.session.status') IN ('error','stopped','interrupted'))
            LIMIT 1`;
          if (
            !terminal ||
            terminal.turnState !== "completed" ||
            terminal.sessionStatus !== "ready" ||
            terminal.lastError !== null ||
            terminal.checkpointStatus !== "ready" ||
            !terminal.turnId ||
            terminal.receiptTurnId !== terminal.turnId ||
            !terminal.checkpointTurnCount ||
            terminal.checkpointRef !==
              checkpointRefForThreadTurn(
                ThreadId.make(active.threadId),
                terminal.checkpointTurnCount,
              ) ||
            evidence.length !== 1
          )
            return yield* epicError(
              "review-rework-terminal",
              "The failed review does not have a later authorized final checkpoint.",
            );
        }
        if (
          !terminal ||
          terminal.turnState === null ||
          (terminal.claimedAt &&
            (terminal.turnState === "pending" || terminal.turnState === "running") &&
            ["error", "stopped", "interrupted"].includes(terminal.sessionStatus ?? ""))
        ) {
          if (
            terminal?.claimedAt &&
            ["error", "stopped", "interrupted"].includes(terminal.sessionStatus ?? "")
          ) {
            const completedAt = DateTime.formatIso(yield* DateTime.now);
            const result = {
              status: "failed" as const,
              candidateCommitSha: null,
              code: "review-repair-delivery-failed",
              message:
                terminal.lastError ??
                "The authorized repair delivery ended before producing its bound turn evidence.",
            };
            const json = canonicalJson(result);
            yield* input.authorize;
            yield* sql`INSERT INTO main.agent_control_epic_review_repair_results(
            request_id,attempt,result_json,result_digest,completed_at)
            VALUES (${input.rework.requestId},${active.attempt},${json},${sha256Utf8(json)},${completedAt})
            ON CONFLICT(request_id,attempt) DO NOTHING`;
            return {
              kind: "repairing" as const,
              attempts: attempts.map((attempt) =>
                attempt.attempt === active.attempt
                  ? {
                      ...attempt,
                      status: "failed" as const,
                      completedAt,
                      error: { code: result.code, message: result.message },
                    }
                  : attempt,
              ),
            };
          }
          yield* ensureDispatched(active);
          return { kind: "repairing" as const, attempts };
        }
        if (terminal.turnState === "pending" || terminal.turnState === "running")
          return { kind: "repairing" as const, attempts };
        if (
          terminal.turnState === "completed" &&
          terminal.sessionStatus === "ready" &&
          terminal.lastError === null &&
          (terminal.checkpointStatus === null ||
            (terminal.checkpointStatus === "missing" &&
              terminal.checkpointRef?.startsWith("provider-diff:")))
        )
          return { kind: "repairing" as const, attempts };
        if (
          terminal.turnState === "completed" &&
          terminal.sessionStatus === "ready" &&
          terminal.checkpointStatus === "ready" &&
          terminal.checkpointRef &&
          terminal.turnId &&
          terminal.receiptTurnId === terminal.turnId
        ) {
          const { branchRef, worktreePath } = activeLocation;
          yield* input.authorize;
          const head = yield* git(worktreePath, ["rev-parse", "HEAD"]);
          if (
            terminal.checkpointTurnCount !== 1 ||
            terminal.checkpointRef !== checkpointRefForThreadTurn(ThreadId.make(active.threadId), 1)
          )
            return yield* epicError(
              "authority-conflict",
              "Repair checkpoint belongs to a different turn.",
            );
          const [candidateTree, reviewedTree, baselineTree] = yield* Effect.all([
            git(worktreePath, ["rev-parse", "--verify", `${terminal.checkpointRef}^{tree}`]),
            git(worktreePath, ["rev-parse", `${input.rework.reviewedCommitSha}^{tree}`]),
            git(worktreePath, [
              "rev-parse",
              "--verify",
              `${checkpointRefForThreadTurn(ThreadId.make(active.threadId), 0)}^{tree}`,
            ]),
          ]);
          if (!objectId.test(candidateTree) || baselineTree !== reviewedTree)
            return yield* epicError(
              "authority-conflict",
              "Repair checkpoint baseline differs from the reviewed commit.",
            );
          // Checkpoints are parentless snapshots. Bind their exact tree to the
          // reviewed commit, with stable metadata so a crash recreates the same candidate.
          const candidate = yield* git(
            worktreePath,
            [
              "commit-tree",
              candidateTree,
              "-p",
              input.rework.reviewedCommitSha,
              "-m",
              `T3Auto review repair ${input.rework.requestId} attempt ${active.attempt}`,
            ],
            {
              GIT_AUTHOR_NAME: "T3Auto",
              GIT_AUTHOR_EMAIL: "t3auto@localhost",
              GIT_COMMITTER_NAME: "T3Auto",
              GIT_COMMITTER_EMAIL: "t3auto@localhost",
              GIT_AUTHOR_DATE: active.createdAt,
              GIT_COMMITTER_DATE: active.createdAt,
            },
          );
          yield* git(projectCwd, [
            "merge-base",
            "--is-ancestor",
            input.rework.reviewedCommitSha,
            head,
          ]);
          yield* input.authorize;
          yield* git(projectCwd, ["update-ref", branchRef, candidate, head]);
          yield* git(worktreePath, ["reset", "--hard", candidate]);
          if (candidateTree === reviewedTree) {
            if (recovering)
              return yield* epicError(
                "review-rework-terminal",
                "The retained checkpoint contains no repair changes.",
              );
            const completedAt = DateTime.formatIso(yield* DateTime.now);
            const result = {
              status: "failed" as const,
              candidateCommitSha: null,
              code: "review-repair-no-changes",
              message:
                "The repair turn completed without producing a change to the reviewed commit.",
            };
            const json = canonicalJson(result);
            yield* input.authorize;
            yield* sql`INSERT INTO main.agent_control_epic_review_repair_results(
            request_id,attempt,result_json,result_digest,completed_at)
            VALUES (${input.rework.requestId},${active.attempt},${json},${sha256Utf8(json)},${completedAt})
            ON CONFLICT(request_id,attempt) DO NOTHING`;
            return {
              kind: "repairing" as const,
              attempts: attempts.map((attempt) =>
                attempt.attempt === active.attempt
                  ? {
                      ...attempt,
                      status: "failed" as const,
                      completedAt,
                      error: { code: result.code, message: result.message },
                    }
                  : attempt,
              ),
            };
          } else {
            const completedAt = DateTime.formatIso(yield* DateTime.now);
            const result = {
              status: "succeeded" as const,
              candidateCommitSha: candidate,
              code: null,
              message: null,
            };
            const json = canonicalJson(result);
            yield* input.authorize;
            if (recovering) {
              yield* sql`INSERT INTO main.agent_control_epic_review_repair_recoveries(
                request_id,attempt,original_result_digest,result_json,result_digest,completed_at)
                VALUES (${input.rework.requestId},${active.attempt},${active.originalResultDigest},
                  ${json},${sha256Utf8(json)},${completedAt})
                ON CONFLICT(request_id,attempt) DO NOTHING`;
            } else {
              yield* sql`INSERT INTO main.agent_control_epic_review_repair_results(
              request_id,attempt,result_json,result_digest,completed_at)
              VALUES (${input.rework.requestId},${active.attempt},${json},${sha256Utf8(json)},${completedAt})
              ON CONFLICT(request_id,attempt) DO NOTHING`;
            }
            const nextAttempts = attempts.map((attempt) =>
              attempt.attempt === active.attempt
                ? { ...attempt, status: "succeeded" as const, completedAt, error: null }
                : attempt,
            );
            return { kind: "candidate" as const, attempts: nextAttempts, commitSha: candidate };
          }
        } else {
          const completedAt = DateTime.formatIso(yield* DateTime.now);
          const result = {
            status: "failed" as const,
            candidateCommitSha: null,
            code: "review-repair-turn-failed",
            message:
              terminal.lastError ?? "The repair provider turn did not complete successfully.",
          };
          const json = canonicalJson(result);
          yield* input.authorize;
          yield* sql`INSERT INTO main.agent_control_epic_review_repair_results(
          request_id,attempt,result_json,result_digest,completed_at)
          VALUES (${input.rework.requestId},${active.attempt},${json},${sha256Utf8(json)},${completedAt})
          ON CONFLICT(request_id,attempt) DO NOTHING`;
          return {
            kind: "repairing" as const,
            attempts: attempts.map((attempt) =>
              attempt.attempt === active.attempt
                ? {
                    ...attempt,
                    status: "failed" as const,
                    completedAt,
                    error: { code: result.code, message: result.message },
                  }
                : attempt,
            ),
          };
        }
      }

      const preflight = yield* policy
        .preflightRuntime({ projectId: input.state.projectId })
        .pipe(
          Effect.mapError(() =>
            epicError(
              "runtime-policy-unavailable",
              "The configured review repair provider route is unavailable.",
            ),
          ),
        );
      const runtimeRole = preflight.roles.find((role) => role.role === "repair");
      const staticRole = preflight.staticPreflight.roles.find((role) => role.role === "repair");
      if (!preflight.ok || !runtimeRole || !staticRole)
        return {
          kind: "blocked" as const,
          attempts,
          code: "runtime-policy-unavailable",
          message: "The configured review repair provider route is unavailable.",
        };
      const attempted = new Set(rows.map((row) => `${row.providerInstanceId}\u0000${row.model}`));
      // Strict routes omit default fallbacks during policy resolution, but may
      // still contain multiple explicitly ordered candidates. A terminal turn
      // advances through every ready candidate in that resolved route once.
      const eligibleIndexes = runtimeRole.candidates
        .filter((candidate) => candidate.runtimeReady)
        .map((candidate) => candidate.candidateIndex);
      const candidateIndex = eligibleIndexes.find((index) => {
        const selection = staticRole.validCandidates[index]?.selection;
        return selection && !attempted.has(`${selection.instanceId}\u0000${selection.model}`);
      });
      const selection: ModelSelection | undefined =
        candidateIndex === undefined
          ? undefined
          : staticRole.validCandidates[candidateIndex]?.selection;
      if (!selection)
        return {
          kind: "blocked" as const,
          attempts,
          code: rows.length ? "review-repair-budget-exhausted" : "runtime-policy-unavailable",
          message: rows.length
            ? "Every configured repair candidate was attempted; the bounded repair budget is exhausted."
            : "No configured repair provider is currently ready.",
        };

      const attempt = rows.length + 1;
      const { branchName, worktreePath } = repairLocation(attempt);
      const threadId = ThreadId.make(`epic-review-${suffix}-${attempt}`);
      const turnRequestCommandId = `epic-review-turn:${input.rework.requestId}:${attempt}`;
      const messageId = `epic-review-message:${input.rework.requestId}:${attempt}`;
      const timestamp = DateTime.formatIso(yield* DateTime.now);
      const runtimeMode =
        runtimeRole.accessMode === "full-access"
          ? ("full-access" as const)
          : ("approval-required" as const);
      const intent = {
        requestId: input.rework.requestId,
        attempt,
        reviewedCommitSha: input.rework.reviewedCommitSha,
        findings: input.rework.findings,
        selection,
        runtimeMode,
        threadId,
        turnRequestCommandId,
        messageId,
        worktreePath,
        branchName,
      };
      const intentJson = canonicalJson(intent);
      yield* input.authorize;
      yield* sql`INSERT INTO main.agent_control_epic_review_repair_intents(
      request_id,attempt,intent_json,intent_digest,provider_instance_id,model,runtime_mode,thread_id,
      turn_request_command_id,message_id,worktree_path,branch_name,created_at) VALUES (
      ${input.rework.requestId},${attempt},${intentJson},${sha256Utf8(intentJson)},
      ${selection.instanceId},${selection.model},${runtimeMode},${threadId},${turnRequestCommandId},
      ${messageId},${worktreePath},${branchName},${timestamp})`;
      yield* ensureDispatched({
        attempt,
        providerInstanceId: selection.instanceId,
        model: selection.model,
        runtimeMode,
        threadId,
        turnRequestCommandId,
        messageId,
        createdAt: timestamp,
        worktreePath,
        branchName,
      });
      return {
        kind: "repairing" as const,
        attempts: [
          ...attempts,
          {
            attempt,
            providerInstanceId: selection.instanceId,
            model: selection.model,
            threadId,
            status: "running" as const,
            startedAt: timestamp,
            completedAt: null,
            error: null,
          },
        ],
      };
    },
    Effect.mapError((cause) =>
      isEpicError(cause)
        ? cause
        : epicError("review-repair-unavailable", "Review repair could not make safe progress."),
    ),
  );

  const cancel = Effect.fn("AgentControlEpicReviewRepair.cancel")(
    function* (input: {
      readonly state: AgentControlEpicReviewRepairInput["state"];
      readonly rework: AgentControlEpicReviewRepairInput["rework"];
    }) {
      const pending = yield* sql<{ attempt: number; threadId: string }>`
      SELECT intent.attempt,intent.thread_id AS "threadId"
      FROM main.agent_control_epic_review_repair_intents intent
      LEFT JOIN main.agent_control_epic_review_repair_results result
        ON result.request_id=intent.request_id AND result.attempt=intent.attempt
      WHERE intent.request_id=${input.rework.requestId} AND result.request_id IS NULL
      ORDER BY intent.attempt`;
      yield* Effect.forEach(
        pending,
        (attempt) =>
          orchestration.dispatch({
            type: "thread.turn.interrupt",
            commandId: CommandId.make(
              `epic-review-interrupt:${input.rework.requestId}:${attempt.attempt}`,
            ),
            threadId: ThreadId.make(attempt.threadId),
            createdAt: input.rework.updatedAt,
          }),
        { concurrency: 1, discard: true },
      );
    },
    Effect.mapError(() =>
      epicError(
        "review-repair-cancel-failed",
        "The retained repair thread could not yet be interrupted.",
      ),
    ),
  );

  return AgentControlEpicReviewRepair.of({
    progress,
    recover: (input) => progress(input, true),
    cancel,
  });
});

export const EpicReviewRepairLive = Layer.effect(AgentControlEpicReviewRepair, make);
