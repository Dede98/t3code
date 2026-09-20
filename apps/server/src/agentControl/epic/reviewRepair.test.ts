// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import {
  ProjectId,
  ThreadId,
  AgentControlEpicRpcError,
  ProviderDriverKind,
  ProviderInstanceId,
  type AgentControlEpicReviewRework,
  type AgentControlEpicSource,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlPolicyService } from "../AgentControlPolicyService.ts";
import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import { checkpointRefForThreadTurn } from "../../checkpointing/Utils.ts";
import { EpicReviewRepairLive } from "./reviewRepair.ts";
import { createEpicRun, insertEpicRun } from "./runState.ts";
import { AgentControlEpicReviewRepair } from "./Services/AgentControlEpicReviewRepair.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const at = "2026-09-18T09:00:00.000Z";
const projectId = ProjectId.make("review-repair-route-project");
const repository = { repositoryNodeId: "review-repair-repository", nameWithOwner: "owner/repo" };
const issue = (number: number) => ({
  ...repository,
  issueNodeId: `issue-${number}`,
  number,
  title: `Issue ${number}`,
  url: `https://github.com/owner/repo/issues/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});
const source: AgentControlEpicSource = {
  format: "github-native-sub-issues-v1",
  repository,
  epic: { ...issue(10), subIssueCount: 1 },
  tasks: [{ issue: issue(11), position: 0, dependencies: [] }],
  blockers: [],
  fingerprint: "review-repair-route-scope",
  inspectedAt: at,
};

for (const recover of [false, true, "cancelled", "final-error"] as const)
  it.effect(
    recover
      ? `recovers only a falsely failed owned checkpoint without replaying repair or rewriting history (${recover})`
      : "waits for final checkpoints and advances strict repair candidates with durable replay identities",
    () =>
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.tryPromise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "epic-review-"))),
          (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
        );
        const git = (args: ReadonlyArray<string>, cwd = directory) =>
          Effect.tryPromise(async () => (await exec("git", [...args], { cwd })).stdout.trim());
        yield* git(["init", "-b", "main"]);
        yield* git(["config", "user.name", "Fixture"]);
        yield* git(["config", "user.email", "fixture@example.test"]);
        yield* Effect.tryPromise(() =>
          NodeFSP.writeFile(NodePath.join(directory, "file.txt"), "base\n"),
        );
        yield* git(["add", "file.txt"]);
        yield* git(["commit", "-m", "reviewed result"]);
        const reviewedCommitSha = yield* git(["rev-parse", "HEAD"]);

        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 95 });
        yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,created_at,updated_at,scripts_json)
        VALUES (${projectId},'Review repair route',${directory},${at},${at},'[]')`;
        const state = yield* createEpicRun({
          projectId,
          commandId: "review-repair-route-run",
          source,
          checks: [],
          initialBase: { commitSha: reviewedCommitSha, targetBranch: "main" },
        });
        yield* sql.withTransaction(insertEpicRun(sql, state));
        const requestId = "review-repair-route-request";
        yield* sql`INSERT INTO agent_control_epic_review_requests(
        request_id,project_id,epic_run_id,idempotency_key,command_id,request_digest,request_json,
        reviewed_commit_sha,reviewed_verification_evidence_id,accepted_revision,accepted_at)
        VALUES (${requestId},${projectId},${state.epicRunId},'route-key','route-command','route-digest','{}',
          ${reviewedCommitSha},'reviewed-proof',2,${at})`;
        const rework: AgentControlEpicReviewRework = {
          requestId,
          idempotencyKey: "route-key",
          reviewedCommitSha,
          reviewedVerificationEvidenceId: "reviewed-proof",
          findings: [
            {
              findingId: "strict-route",
              summary: "The first repair candidate cannot complete the requested correction.",
              correctionCriteria: "Use the next explicitly configured candidate.",
              acceptanceCriteria: "The second candidate starts from the reviewed commit.",
            },
          ],
          status: "accepted",
          previousAcceptedCommitSha: reviewedCommitSha,
          previousVerificationEvidenceId: "reviewed-proof",
          repairAttempts: [],
          candidateCommitSha: null,
          verification: null,
          blocker: null,
          requestedAt: at,
          updatedAt: at,
          completedAt: null,
        };
        const firstProvider = ProviderInstanceId.make("strict-repair-first");
        const secondProvider = ProviderInstanceId.make("strict-repair-second");
        const selections = [
          { instanceId: firstProvider, model: "repair-first" },
          { instanceId: secondProvider, model: "repair-second" },
        ];
        const policy = AgentControlPolicyService.of({
          getPolicy: () => Effect.die("unused"),
          setProjectPolicy: () => Effect.die("unused"),
          clearProjectPolicy: () => Effect.die("unused"),
          preflightPolicy: () => Effect.die("unused"),
          preflightRuntime: () =>
            Effect.succeed({
              ok: true,
              staticPreflight: {
                ok: true,
                roles: [
                  {
                    role: "repair" as const,
                    accessMode: "restricted" as const,
                    strict: true,
                    validCandidates: selections.map((selection) => ({
                      selection,
                      source: "role-route" as const,
                      driverKind: ProviderDriverKind.make("codex"),
                    })),
                  },
                ],
              },
              roles: [
                {
                  role: "repair" as const,
                  accessMode: "restricted" as const,
                  strict: true,
                  candidates: selections.map((selection, candidateIndex) => ({
                    candidateIndex,
                    source: "role-route" as const,
                    providerInstanceId: selection.instanceId,
                    model: selection.model,
                    driverKind: ProviderDriverKind.make("codex"),
                    providerStatus: "ready" as const,
                    authStatus: "authenticated" as const,
                    checkedAt: at,
                    runtimeReady: true,
                    errorCode: null,
                  })),
                  selectedCandidateIndex: 0,
                  errorCode: null,
                },
              ],
            }),
        });
        const commands: {
          readonly type: string;
          readonly commandId: string;
          readonly threadId: string;
        }[] = [];
        const seenCommands = new Set<string>();
        const orchestration = Layer.mock(OrchestrationEngineService)({
          dispatch: (command) =>
            Effect.sync(() => {
              if (!seenCommands.has(command.commandId)) {
                seenCommands.add(command.commandId);
                if ("threadId" in command)
                  commands.push({
                    type: command.type,
                    commandId: command.commandId,
                    threadId: command.threadId,
                  });
              }
              return { sequence: seenCommands.size };
            }),
        });
        const repairLayer = EpicReviewRepairLive.pipe(
          Layer.provide(
            Layer.merge(Layer.succeed(AgentControlPolicyService, policy), orchestration),
          ),
        );
        const build = () => AgentControlEpicReviewRepair.pipe(Effect.provide(repairLayer));
        const input = { state, rework, authorize: Effect.void };

        const firstService = yield* build();
        const first = yield* firstService.progress(input);
        assert.equal(first.kind, "repairing");
        assert.equal(first.attempts[0]?.providerInstanceId, firstProvider);
        assert.equal(first.attempts[0]?.status, "running");
        assert.lengthOf(
          commands.filter((command) => command.type === "thread.turn.start"),
          1,
        );

        // A rebuilt service reuses the durable command/thread identity. The mock
        // models orchestration command receipts by recording each command id once.
        const replayService = yield* build();
        const replay = yield* replayService.progress(input);
        assert.equal(replay.attempts[0]?.threadId, first.attempts[0]?.threadId);
        assert.lengthOf(
          commands.filter((command) => command.type === "thread.turn.start"),
          1,
        );
        assert.deepEqual(
          yield* sql`SELECT attempt,provider_instance_id AS providerInstanceId
          FROM agent_control_epic_review_repair_intents ORDER BY attempt`,
          [{ attempt: 1, providerInstanceId: firstProvider }],
        );

        const failedResult = canonicalJson({
          status: "failed",
          candidateCommitSha: null,
          code: "review-repair-turn-failed",
          message: "The first explicit candidate failed.",
        });
        yield* sql`INSERT INTO agent_control_epic_review_repair_results(
        request_id,attempt,result_json,result_digest,completed_at)
        VALUES (${requestId},1,${failedResult},${sha256Utf8(failedResult)},${at})`;

        const secondService = yield* build();
        const second = yield* secondService.progress(input);
        assert.equal(second.kind, "repairing");
        assert.deepEqual(
          second.attempts.map((attempt) => [
            attempt.attempt,
            attempt.providerInstanceId,
            attempt.status,
          ]),
          [
            [1, firstProvider, "failed"],
            [2, secondProvider, "running"],
          ],
        );
        const secondWorktree = yield* sql<{
          worktreePath: string;
        }>`SELECT worktree_path AS "worktreePath"
        FROM agent_control_epic_review_repair_intents WHERE request_id=${requestId} AND attempt=2`;
        assert.equal(
          yield* git(["rev-parse", "HEAD"], secondWorktree[0]!.worktreePath),
          reviewedCommitSha,
        );
        assert.equal(
          yield* git([
            "rev-parse",
            `refs/heads/t3auto/epic-review-${sha256Utf8(requestId).slice(0, 24)}-2`,
          ]),
          reviewedCommitSha,
        );
        assert.lengthOf(
          commands.filter((command) => command.type === "thread.turn.start"),
          2,
        );
        yield* (yield* build()).progress(input);
        assert.lengthOf(
          commands.filter((command) => command.type === "thread.turn.start"),
          2,
        );

        const secondPath = secondWorktree[0]!.worktreePath;
        yield* Effect.tryPromise(() =>
          NodeFSP.writeFile(NodePath.join(secondPath, "authorized.txt"), "authorized repair\n"),
        );
        yield* git(["add", "authorized.txt"], secondPath);
        const checkpointTree = yield* git(["write-tree"], secondPath);
        const checkpointCommit = yield* git(
          ["commit-tree", checkpointTree, "-m", "authorized checkpoint"],
          secondPath,
        );
        const checkpointRef = checkpointRefForThreadTurn(
          ThreadId.make(second.attempts[1]!.threadId),
          1,
        );
        yield* git(["update-ref", checkpointRef, checkpointCommit]);
        const baselineRef = checkpointRefForThreadTurn(
          ThreadId.make(second.attempts[1]!.threadId),
          0,
        );
        yield* git(["update-ref", baselineRef, reviewedCommitSha]);
        yield* git(["reset", "--hard", reviewedCommitSha], secondPath);
        yield* Effect.tryPromise(() =>
          NodeFSP.writeFile(
            NodePath.join(secondPath, "later-unowned.txt"),
            "must not be accepted\n",
          ),
        );
        const secondIntent = yield* sql<{
          threadId: string;
          messageId: string;
        }>`SELECT thread_id AS "threadId",message_id AS "messageId"
        FROM agent_control_epic_review_repair_intents
        WHERE request_id=${requestId} AND attempt=2`;
        yield* sql`INSERT INTO agent_control_epic_review_repair_delivery_claims(
        request_id,attempt,claimed_at) VALUES (${requestId},2,${at})`;
        yield* sql`INSERT INTO agent_control_epic_review_repair_delivery_receipts(
        request_id,attempt,provider_turn_id,accepted_at)
        VALUES (${requestId},2,'review-provider-turn',${at})`;
        yield* sql`INSERT INTO projection_threads(
        thread_id,project_id,title,model_selection_json,runtime_mode,interaction_mode,
        branch,worktree_path,created_at,updated_at)
        VALUES (${secondIntent[0]!.threadId},${projectId},'Repair',
          '{"provider":"codex","model":"repair-second"}','approval-required','default',
          't3auto/review',${secondPath},${at},${at})`;
        yield* sql`INSERT INTO projection_thread_sessions(
        thread_id,status,provider_name,active_turn_id,last_error,updated_at)
        VALUES (${secondIntent[0]!.threadId},'ready','codex',NULL,NULL,${at})`;
        yield* sql`INSERT INTO projection_turns(
        thread_id,turn_id,pending_message_id,state,requested_at,started_at,completed_at,
        checkpoint_turn_count,checkpoint_ref,checkpoint_status,checkpoint_files_json)
        VALUES (${secondIntent[0]!.threadId},'review-provider-turn',${secondIntent[0]!.messageId},
          'completed',${at},${at},${at},1,${checkpointRef},'ready','[]')`;
        // A completed provider session precedes projection of the final Git checkpoint.
        for (const status of [null, "missing"] as const) {
          yield* sql`UPDATE projection_turns SET checkpoint_status=${status},
          checkpoint_ref='provider-diff:in-flight' WHERE thread_id=${secondIntent[0]!.threadId}`;
          const waiting = yield* (yield* build()).progress(input);
          assert.equal(waiting.kind, "repairing");
          assert.equal(waiting.attempts.at(-1)?.status, "running");
          assert.deepEqual(
            yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_results
          WHERE request_id=${requestId} AND attempt=2`,
            [{ count: 0 }],
          );
        }
        yield* sql`UPDATE projection_turns SET checkpoint_status='ready',checkpoint_ref=${checkpointRef}
        WHERE thread_id=${secondIntent[0]!.threadId}`;
        if (recover) {
          const falseFailure = canonicalJson({
            status: "failed",
            candidateCommitSha: null,
            code: "review-repair-turn-failed",
            message: "The repair provider turn did not complete successfully.",
          });
          yield* sql`INSERT INTO agent_control_epic_review_repair_results(
          request_id,attempt,result_json,result_digest,completed_at)
          VALUES (${requestId},2,${falseFailure},${sha256Utf8(falseFailure)},${at})`;
          const recoverService = yield* build();
          const recoverAttempt = () => recoverService.recover!(input);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          const event = (
            sequence: number,
            status: string,
            ref: string,
          ) => sql`INSERT INTO orchestration_events(
          event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json)
          VALUES (${`checkpoint-${sequence}`},'thread',${secondIntent[0]!.threadId},${sequence},
            'thread.turn-diff-completed',${at},'server',${canonicalJson({
              threadId: secondIntent[0]!.threadId,
              turnId: "review-provider-turn",
              status,
              checkpointRef: ref,
              checkpointTurnCount: 1,
              files: [],
              assistantMessageId: null,
              completedAt: at,
            })},'{}')`;
          yield* event(1, "missing", "provider-diff:in-flight");
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* event(2, "ready", checkpointRef);
          for (const status of ["error", "missing"]) {
            yield* sql`UPDATE projection_turns SET checkpoint_status=${status} WHERE thread_id=${secondIntent[0]!.threadId}`;
            assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          }
          yield* sql`UPDATE projection_turns SET checkpoint_status='ready',turn_id='wrong-receipt'
          WHERE thread_id=${secondIntent[0]!.threadId}`;
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* sql`UPDATE projection_turns SET turn_id='review-provider-turn',checkpoint_ref='refs/heads/main'
          WHERE thread_id=${secondIntent[0]!.threadId}`;
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* sql`UPDATE projection_turns SET checkpoint_ref=${checkpointRef} WHERE thread_id=${secondIntent[0]!.threadId}`;
          yield* sql`UPDATE projection_thread_sessions SET last_error='provider failed' WHERE thread_id=${secondIntent[0]!.threadId}`;
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* sql`UPDATE projection_thread_sessions SET last_error=NULL WHERE thread_id=${secondIntent[0]!.threadId}`;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                recoverService.recover!({
                  ...input,
                  authorize: Effect.fail(
                    new AgentControlEpicRpcError({
                      code: "authority-conflict",
                      message: "revoked",
                    }),
                  ),
                }),
              ),
            ),
          );
          yield* git(["update-ref", baselineRef, checkpointCommit]);
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* git(["update-ref", baselineRef, reviewedCommitSha]);
          yield* sql`UPDATE projection_threads SET worktree_path='/different-worktree' WHERE thread_id=${secondIntent[0]!.threadId}`;
          assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
          yield* sql`UPDATE projection_threads SET worktree_path=${secondPath} WHERE thread_id=${secondIntent[0]!.threadId}`;
          if (recover === "cancelled" || recover === "final-error") {
            if (recover === "cancelled")
              yield* sql`INSERT INTO agent_control_epic_review_repair_cancellations(request_id,attempt,cancelled_at)
                VALUES (${requestId},2,${at})`;
            else yield* event(3, "error", checkpointRef);
            assert.isTrue(Exit.isFailure(yield* Effect.exit(recoverAttempt())));
            assert.deepEqual(
              yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_recoveries`,
              [{ count: 0 }],
            );
            assert.lengthOf(
              commands.filter((command) => command.type === "thread.turn.start"),
              2,
            );
            return;
          }
          assert.deepEqual(
            yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_recoveries`,
            [{ count: 0 }],
          );
        }
        let interruptedCandidate: string | undefined;
        if (!recover) {
          let authorizations = 0;
          const interruption = yield* (yield* build())
            .progress({
              ...input,
              authorize: Effect.suspend(() =>
                ++authorizations === 4
                  ? Effect.fail(
                      new AgentControlEpicRpcError({
                        code: "authority-conflict",
                        message: "Interrupted before persisting the result",
                      }),
                    )
                  : Effect.void,
              ),
            })
            .pipe(Effect.exit);
          assert.isTrue(Exit.isFailure(interruption));
          interruptedCandidate = yield* git(["rev-parse", "HEAD"], secondPath);
          assert.notEqual(interruptedCandidate, reviewedCommitSha);
          assert.deepEqual(
            yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_results
            WHERE request_id=${requestId} AND attempt=2`,
            [{ count: 0 }],
          );
        }
        const service = yield* build();
        const completed = yield* recover ? service.recover!(input) : service.progress(input);
        assert.equal(completed.kind, "candidate");
        if (completed.kind !== "candidate") return yield* Effect.die("candidate missing");
        if (interruptedCandidate) assert.equal(completed.commitSha, interruptedCandidate);
        assert.notEqual(completed.commitSha, checkpointCommit);
        assert.equal(yield* git(["rev-parse", `${completed.commitSha}^{tree}`]), checkpointTree);
        assert.equal(yield* git(["rev-parse", `${completed.commitSha}^`]), reviewedCommitSha);
        assert.equal(
          yield* git(["show", `${completed.commitSha}:authorized.txt`]),
          "authorized repair",
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(git(["show", `${completed.commitSha}:later-unowned.txt`])),
          ),
        );

        const restarted = yield* (yield* build()).progress(input);
        assert.deepEqual(restarted, completed);
        assert.lengthOf(
          commands.filter((command) => command.type === "thread.turn.start"),
          2,
        );
        if (recover) {
          const recoveredReplay = yield* (yield* build()).recover!(input);
          assert.deepEqual(recoveredReplay, completed);
          assert.deepEqual(
            yield* sql`SELECT json_extract(result_json,'$.status') AS status
          FROM agent_control_epic_review_repair_results WHERE request_id=${requestId} AND attempt=2`,
            [{ status: "failed" }],
          );
          assert.deepEqual(
            yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_recoveries`,
            [{ count: 1 }],
          );
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sql`UPDATE agent_control_epic_review_repair_recoveries SET completed_at='changed'`,
              ),
            ),
          );
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(sql`DELETE FROM agent_control_epic_review_repair_recoveries`),
            ),
          );
        }

        const blockedRequestId = "review-repair-ambiguous-request";
        yield* sql`INSERT INTO agent_control_epic_review_requests(
        request_id,project_id,epic_run_id,idempotency_key,command_id,request_digest,request_json,
        reviewed_commit_sha,reviewed_verification_evidence_id,accepted_revision,accepted_at)
        VALUES (${blockedRequestId},${projectId},${state.epicRunId},'ambiguous-key',
          'ambiguous-command','ambiguous-digest','{}',${reviewedCommitSha},'reviewed-proof',3,${at})`;
        const blockedRework = {
          ...rework,
          requestId: blockedRequestId,
          idempotencyKey: "ambiguous-key",
        };
        const blockedInput = { ...input, rework: blockedRework };
        yield* (yield* build()).progress(blockedInput);
        const ambiguousResult = canonicalJson({
          status: "blocked",
          candidateCommitSha: null,
          code: "review-repair-delivery-ambiguous",
          message: "The accepted provider outcome is unknown after restart.",
        });
        yield* sql`INSERT INTO agent_control_epic_review_repair_results(
        request_id,attempt,result_json,result_digest,completed_at)
        VALUES (${blockedRequestId},1,${ambiguousResult},${sha256Utf8(ambiguousResult)},${at})`;
        const blocked = yield* (yield* build()).progress(blockedInput);
        assert.equal(blocked.kind, "blocked");
        assert.equal(blocked.attempts[0]?.status, "stopped");
        assert.deepEqual(
          yield* sql`SELECT count(*) AS count FROM agent_control_epic_review_repair_intents
          WHERE request_id=${blockedRequestId}`,
          [{ count: 1 }],
        );

        const immutableIntent = yield* Effect.exit(
          sql`UPDATE agent_control_epic_review_repair_intents SET model='other' WHERE request_id=${requestId} AND attempt=1`,
        );
        const immutableResult = yield* Effect.exit(
          sql`UPDATE agent_control_epic_review_repair_results SET completed_at='later' WHERE request_id=${requestId} AND attempt=1`,
        );
        assert.isTrue(Exit.isFailure(immutableIntent));
        assert.isTrue(Exit.isFailure(immutableResult));
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
