import { unsettledEpicExecutions } from "../executionState.ts";
import { withAgentControlRunOnceProjectFence } from "../../runOnce/context.ts";
import { epicDependenciesSatisfied, validateEpicDependencyPlan } from "../dependencyPlan.ts";
import { AgentControlPolicyService } from "../../AgentControlPolicyService.ts";
import { makeEpicQueue, mapEpicQueueError } from "../queue.ts";
import { loadEnabledEpicQueue } from "../queueAuthority.ts";
import { GithubIssueTrackerClientError } from "../../github/Services/GithubIssueTrackerClient.ts";
import { epicIssueContentFingerprint } from "../../github/githubEpicSource.ts";
import { createEpicRun, insertEpicRun } from "../runState.ts";
import {
  AgentControlEpicRpcError,
  AgentControlProjectPolicy,
  AgentControlTaskId,
  CommandId,
  type AgentControlEpicQueueChangeInput,
  type AgentControlEpicAcceptedResult,
  type AgentControlEpicMemberView,
  type AgentControlEpicFinalVerification,
  type AgentControlEpicBlocker,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicControlInput,
  type AgentControlEpicStartInput,
  type AgentControlEpicPreviewInput,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { GithubIssueTrackerClient } from "../../github/Services/GithubIssueTrackerClient.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlRunOnceReadNotifications } from "../../runOnce/readNotifications.ts";
import { makeAgentControlRunOnceKeyedFence } from "../../runOnce/context.ts";
import { AgentControlEpic } from "../Services/AgentControlEpic.ts";
import { AgentControlEpicResultHooks } from "../Services/AgentControlEpicResultHooks.ts";
import {
  epicDigest,
  epicError,
  loadEpicRun,
  loadSelectedEpic,
  saveEpicRun,
  requireEpicIntegrationAuthority,
} from "../authority.ts";
import { epicSourceChanges, selectEpicMember } from "../model.ts";
import { makeEpicHandoff } from "../handoff.ts";
import { EpicHandoffEvidence } from "../handoffAuthority.ts";
import { EpicHandoffRemote } from "../remote.ts";

const isEpicError = Schema.is(AgentControlEpicRpcError);
const isGithubError = Schema.is(GithubIssueTrackerClientError);
const mapError = (cause: unknown) =>
  isEpicError(cause)
    ? cause
    : isGithubError(cause)
      ? epicError(
          "source-unavailable",
          "GitHub could not provide complete Epic source. Check access and retry.",
        )
      : epicError(
          "epic-unavailable",
          "Epic execution could not obtain complete source or persistence authority.",
        );
const decodePolicy = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlProjectPolicy));

export const makeAgentControlEpic = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const client = yield* GithubIssueTrackerClient;
  const github = yield* AgentControlGithubStateRepository;
  const engine = yield* AgentControlEngine;
  const policyService = yield* AgentControlPolicyService;
  const results = yield* AgentControlEpicResultHooks;
  const notifications = yield* AgentControlRunOnceReadNotifications;
  const changes = yield* PubSub.unbounded<ProjectId>();
  const publish = (projectId: ProjectId) =>
    notifications
      .publishProject(projectId)
      .pipe(Effect.andThen(PubSub.publish(changes, projectId)), Effect.asVoid);
  const locks = makeAgentControlRunOnceKeyedFence<ProjectId>();
  const queue = yield* makeEpicQueue;
  const get = (projectId: ProjectId) =>
    loadSelectedEpic(sql, projectId).pipe(Effect.mapError(mapError));

  const inspect = Effect.fn("AgentControlEpic.inspect")(function* (
    input: AgentControlEpicPreviewInput,
  ) {
    const state = yield* github.get(input.projectId);
    if (Option.isNone(state) || !state.value.config || !client.inspectEpic)
      return yield* epicError(
        "source-unavailable",
        "Configure GitHub intake with native Epic support first.",
      );
    const projects = yield* sql<{
      cwd: string;
    }>`SELECT workspace_root AS cwd FROM projection_projects WHERE project_id=${input.projectId} AND deleted_at IS NULL`;
    if (!projects[0])
      return yield* epicError(
        "project-unavailable",
        "The selected environment no longer has this project.",
      );
    const repository = state.value.config.repository;
    const [owner, name] = repository.nameWithOwner.split("/");
    if (!owner || !name)
      return yield* epicError(
        "source-unavailable",
        "The configured repository identity is invalid.",
      );
    return yield* client.inspectEpic({
      cwd: projects[0].cwd,
      locator: { owner, name },
      expectedRepository: repository,
      epicNumber: input.epicNumber,
    });
  });

  const sourceTasks = Effect.fn("AgentControlEpic.sourceTasks")(function* (projectId: ProjectId) {
    const snapshot = yield* github.getCompletedSnapshot(projectId);
    if (Option.isNone(snapshot))
      return yield* epicError(
        "intake-incomplete",
        "A complete successful GitHub intake is required.",
      );
    const reconcile = yield* sql<{
      status: string;
      target: number;
      completed: number;
    }>`SELECT status,target_sequence AS target,last_completed_sequence AS completed FROM agent_control_task_reconcile_states WHERE project_id=${projectId}`;
    const sequence = snapshot.value.sourcePrecondition.githubIntakeSequence;
    if (
      reconcile.length !== 1 ||
      reconcile[0]!.status !== "completed" ||
      reconcile[0]!.target !== sequence ||
      reconcile[0]!.completed !== sequence
    )
      return yield* epicError("intake-incomplete", "GitHub task reconciliation has not completed.");
    return yield* sql<{
      taskId: string;
      issueNodeId: string;
      status: string;
      sourceGate: string;
      stage: string;
      sequence: number;
    }>`SELECT task_id AS "taskId",issue_node_id AS "issueNodeId",status,source_gate AS "sourceGate",stage,github_intake_sequence AS sequence FROM agent_control_task_states WHERE project_id=${projectId} AND github_intake_sequence=${sequence}`;
  });

  const preview = Effect.fn("AgentControlEpic.preview")(function* (
    input: AgentControlEpicPreviewInput,
  ) {
    const source = yield* inspect(input);
    const blockers: Array<AgentControlEpicBlocker> = [...source.blockers];
    const tasks = yield* sourceTasks(input.projectId).pipe(
      Effect.catch((cause) => {
        const error = mapError(cause);
        if (["epic-unavailable", "authority-conflict", "revision-conflict"].includes(error.code))
          return Effect.fail(error);
        blockers.push({ code: error.code, issueNumber: null, message: error.message });
        return Effect.succeed([]);
      }),
    );
    for (const member of source.tasks) {
      if (member.issue.state === "closed") continue;
      const task = tasks.find((task) => task.issueNodeId === member.issue.issueNodeId);
      if (
        !task ||
        task.status !== "candidate" ||
        task.sourceGate !== "eligible" ||
        task.stage !== "intake"
      )
        blockers.push({
          code: task ? "task-not-approved" : "missing-issue",
          issueNumber: member.issue.number,
          message: task
            ? `Issue #${member.issue.number} has no current trusted execution approval or already has execution history.`
            : `Issue #${member.issue.number} is missing from the completed intake.`,
        });
    }
    const policies = yield* sql<{
      policy: string;
    }>`SELECT policy_json AS policy FROM agent_control_project_policies WHERE project_id=${input.projectId}`;
    const policy = yield* decodePolicy(policies[0]?.policy ?? "{}");
    if (!policy.verificationChecks?.some((check) => check.required))
      blockers.push({
        code: "verification-checks-missing",
        issueNumber: null,
        message: "Configure at least one required verification check before starting an Epic.",
      });
    const runtime = yield* policyService.preflightRuntime({ projectId: input.projectId });
    if (!runtime.ok) {
      const failures = runtime.roles.flatMap((role) =>
        role.selectedCandidateIndex === null
          ? role.candidates.flatMap((candidate) =>
              candidate.verificationCheckError
                ? [
                    {
                      code: "verification-checks-unavailable",
                      issueNumber: null,
                      message: `Check ${candidate.verificationCheckError.checkId}: ${candidate.verificationCheckError.message}`,
                    },
                  ]
                : [],
            )
          : [],
      );
      blockers.push(
        ...(failures.length
          ? failures
          : [
              {
                code: "runtime-policy-unavailable",
                issueNumber: null,
                message: "The configured providers are not ready for every required T3Auto role.",
              },
            ]),
      );
    }
    const fatal = new Set([
      "nested-sub-issues",
      "cross-repository",
      "empty-epic",
      "closed-epic",
      "intake-incomplete",
      "verification-checks-missing",
      "verification-checks-unavailable",
      "runtime-policy-unavailable",
      "epic-unavailable",
    ]);
    return {
      projectId: input.projectId,
      source,
      blockers,
      canStart: !blockers.some((item) => fatal.has(item.code)),
    };
  }, Effect.mapError(mapError));

  const persist = (
    previous: AgentControlEpicRuntimeView,
    changes: Partial<AgentControlEpicRuntimeView>,
  ) =>
    sql
      .withTransaction(saveEpicRun(sql, previous, changes))
      .pipe(Effect.tap(() => publish(previous.projectId)));
  const block = (
    state: AgentControlEpicRuntimeView,
    blockers: ReadonlyArray<AgentControlEpicBlocker>,
  ) =>
    persist(state, {
      status: "blocked",
      blockers,
      blockerHistory: [...state.blockerHistory, { recordedAt: state.updatedAt, blockers }],
    });
  const replayCommand = Effect.fn("AgentControlEpic.replayCommand")(function* (
    kind: string,
    input: AgentControlEpicStartInput | AgentControlEpicControlInput,
  ) {
    const rows = yield* sql<{
      runId: string;
      digest: string;
    }>`SELECT epic_run_id AS "runId",request_digest AS digest FROM agent_control_epic_commands WHERE command_id=${input.commandId}`;
    if (!rows[0]) return null;
    if (rows[0].digest !== epicDigest({ kind, input }))
      return yield* epicError(
        "command-conflict",
        "This Epic command identity was already used for another request.",
      );
    return yield* loadEpicRun(sql, rows[0].runId);
  });
  const recordCommand = (
    kind: string,
    input: AgentControlEpicStartInput | AgentControlEpicControlInput,
    runId: string,
  ) =>
    sql`INSERT INTO main.agent_control_epic_commands(command_id,request_digest,epic_run_id) VALUES (${input.commandId},${epicDigest({ kind, input })},${runId})`;

  const modeIntent = (
    state: AgentControlEpicRuntimeView,
    commandId: CommandId,
    expectedRevision: number,
    mode: "armed" | "observe",
  ) =>
    sql`INSERT INTO main.agent_control_epic_mode_intents(command_id,epic_run_id,expected_revision,mode) VALUES (${`epic-mode-${commandId}`},${state.epicRunId},${expectedRevision},${mode})`;
  const recoverModeIntent = Effect.fn("AgentControlEpic.recoverModeIntent")(function* (
    state: AgentControlEpicRuntimeView,
  ) {
    const intents = yield* sql<{
      commandId: string;
      expectedRevision: number;
      mode: "armed" | "observe";
    }>`SELECT command_id AS "commandId",expected_revision AS "expectedRevision",mode FROM main.agent_control_epic_mode_intents WHERE epic_run_id=${state.epicRunId} AND status='pending' ORDER BY rowid`;
    for (const intent of intents) {
      const project = yield* engine.getProjectState({ projectId: state.projectId });
      const committed =
        yield* sql`SELECT 1 FROM main.agent_control_events WHERE command_id=${intent.commandId} AND stream_id=${state.projectId} AND event_type='agentControl.project.mode.changed'`;
      if (committed.length === 0 && project.revision === intent.expectedRevision) {
        yield* engine.dispatchHuman({
          projectId: state.projectId,
          expectedRevision: intent.expectedRevision,
          commandId: CommandId.make(intent.commandId),
          mode: intent.mode,
        });
      } else if (committed.length === 0) {
        // A later human mode command supersedes pending activation, including an explicit off.
        yield* sql`UPDATE main.agent_control_epic_mode_intents SET status='superseded' WHERE command_id=${intent.commandId} AND status='pending'`;
        continue;
      }
      yield* sql`UPDATE main.agent_control_epic_mode_intents SET status='applied' WHERE command_id=${intent.commandId} AND status='pending'`;
    }
  });

  const start = (input: AgentControlEpicStartInput) =>
    locks
      .withPermit(
        input.projectId,
        Effect.gen(function* () {
          const replay = yield* replayCommand("start", input);
          if (replay) {
            yield* recoverModeIntent(replay);
            return replay;
          }
          if (yield* loadEnabledEpicQueue(sql, input.projectId))
            return yield* epicError(
              "queue-enabled",
              "Approve this Epic through the project queue.",
            );
          const inspected = yield* preview(input);
          if (inspected.source.fingerprint !== input.expectedFingerprint)
            return yield* epicError(
              "scope-changed",
              "The Epic changed since preview. Inspect its current scope before starting.",
            );
          if (!inspected.canStart)
            return yield* epicError(
              "start-blocked",
              inspected.blockers.map((item) => item.message).join(" "),
            );
          const policyRows = yield* sql<{
            policy: string;
          }>`SELECT policy_json AS policy FROM agent_control_project_policies WHERE project_id=${input.projectId}`;
          const policy = yield* decodePolicy(policyRows[0]?.policy ?? "{}");
          let initialBase: { commitSha: string; targetBranch: string } | undefined;
          if (input.dependencyPlan) {
            yield* validateEpicDependencyPlan(
              inspected.source,
              input.dependencyPlan,
              input.parallelism,
            );
            if (Option.isNone(handoffRemote) || !handoffRemote.value.refreshQueueBase)
              return yield* epicError(
                "base-unavailable",
                "Fresh target-branch loading is required for a dependency plan.",
              );
            const roots = yield* sql<{
              cwd: string;
            }>`SELECT workspace_root AS cwd FROM main.projection_projects WHERE project_id=${input.projectId} AND deleted_at IS NULL`;
            if (!roots[0])
              return yield* epicError("base-unavailable", "Project directory is unavailable.");
            initialBase = yield* handoffRemote.value.refreshQueueBase({
              cwd: roots[0].cwd,
              repository: inspected.source.repository,
            });
          }
          const state = yield* createEpicRun({
            projectId: input.projectId,
            commandId: input.commandId,
            source: inspected.source,
            ...(initialBase ? { initialBase } : {}),
            checks: policy.verificationChecks ?? [],
            ...(input.parallelism !== undefined ? { parallelism: input.parallelism } : {}),
            ...(input.dependencyPlan ? { dependencyPlan: input.dependencyPlan } : {}),
          });
          const persisted = yield* sql.withTransaction(
            Effect.gen(function* () {
              const replay = yield* replayCommand("start", input);
              if (replay) return replay;
              const existing = yield* loadSelectedEpic(sql, input.projectId);
              if (existing)
                return yield* epicError(
                  "epic-already-selected",
                  "Finish and clear the selected Epic before starting another run.",
                );
              const project = yield* engine.getProjectState({ projectId: input.projectId });
              if (project.revision !== input.expectedRevision)
                return yield* epicError(
                  "revision-conflict",
                  "Project automation changed. Reload before starting the Epic.",
                );
              const activeRuns =
                yield* sql`SELECT 1 FROM main.agent_control_run_once_states WHERE project_id=${input.projectId} AND status='active'`;
              if (activeRuns.length !== 0)
                return yield* epicError(
                  "project-busy",
                  "An admitted Run Once task still owns execution in this project.",
                );
              if (
                project.mode === "run-once" ||
                project.mode === "armed" ||
                project.pausedFromMode === "run-once"
              )
                return yield* epicError(
                  "project-busy",
                  "Turn automation off and finish the active task before selecting an Epic.",
                );
              yield* insertEpicRun(sql, state);
              yield* recordCommand("start", input, state.epicRunId);
              yield* modeIntent(state, input.commandId, input.expectedRevision, "armed");
              return state;
            }),
          );
          yield* publish(input.projectId);
          yield* recoverModeIntent(persisted);
          return persisted;
        }),
      )
      .pipe(Effect.mapError(mapError));

  const control = (kind: "resume" | "stop" | "clear", input: AgentControlEpicControlInput) =>
    locks
      .withPermit(
        input.projectId,
        Effect.gen(function* () {
          const replay = yield* replayCommand(kind, input);
          if (replay) {
            yield* recoverModeIntent(replay);
            return replay;
          }
          const current = yield* get(input.projectId);
          if (!current || current.epicRunId !== input.epicRunId)
            return yield* epicError("epic-missing", "This Epic run is no longer selected.");
          if (current.revision !== input.expectedRevision)
            return yield* epicError(
              "revision-conflict",
              "Epic progress changed. Reload before continuing.",
            );
          if (kind === "resume" && (current.status === "stopped" || current.status === "succeeded"))
            return yield* epicError(
              "epic-terminal",
              "A completed or stopped Epic run cannot be resumed.",
            );
          if (kind === "clear" && (yield* loadEnabledEpicQueue(sql, input.projectId)))
            return yield* epicError(
              "queue-entry-retained",
              "Use Leave Epic queue after turning Armed off and removing waiting entries.",
            );
          if (kind === "clear") {
            yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  const selected = yield* loadSelectedEpic(sql, input.projectId);
                  const project = yield* engine.getProjectState({ projectId: input.projectId });
                  const activeRuns =
                    yield* sql`SELECT 1 FROM main.agent_control_run_once_states WHERE project_id=${input.projectId} AND status='active'`;
                  if (
                    !selected ||
                    selected.epicRunId !== current.epicRunId ||
                    selected.revision !== input.expectedRevision
                  )
                    return yield* epicError(
                      "revision-conflict",
                      "Epic progress changed before clearing its selection.",
                    );
                  if (
                    (selected.status !== "stopped" && selected.status !== "succeeded") ||
                    activeRuns.length !== 0 ||
                    (yield* unsettledEpicExecutions(sql, input.projectId)).size !== 0 ||
                    project.mode === "armed" ||
                    project.mode === "run-once" ||
                    project.pausedFromMode === "run-once"
                  )
                    return yield* epicError(
                      "clear-blocked",
                      "Stop the Epic, turn automation off, and wait for admitted work to settle before returning to ordinary tasks.",
                    );
                  yield* sql`DELETE FROM main.agent_control_epic_targets WHERE project_id=${input.projectId} AND epic_run_id=${input.epicRunId}`;
                  yield* recordCommand(kind, input, current.epicRunId);
                }),
              )
              .pipe((effect) => withAgentControlRunOnceProjectFence(input.projectId, effect));
            yield* publish(input.projectId);
            return current;
          }
          const queued = (yield* loadEnabledEpicQueue(sql, input.projectId)) !== null;
          const modeState = yield* engine.getProjectState({ projectId: input.projectId });
          const updated = yield* sql.withTransaction(
            Effect.gen(function* () {
              const updated = yield* saveEpicRun(
                sql,
                current,
                kind === "stop"
                  ? queued
                    ? {}
                    : { status: "stopped" }
                  : {
                      status: "running",
                      blockers: [],
                      ...(current.dependencyPlan
                        ? {
                            members: current.members.map((member) => {
                              if (member.status !== "failed" || !member.captured) return member;
                              const {
                                blocker: _blocker,
                                waitReason: _waitReason,
                                ...retained
                              } = member;
                              return {
                                ...retained,
                                status: "running" as const,
                                waitReason: "integration" as const,
                              };
                            }),
                          }
                        : {}),
                      verificationAttempt:
                        current.dependencyPlan !== undefined ||
                        current.finalVerification !== null ||
                        (current.acceptedCommitSha !== null &&
                          current.members.every(
                            (member) =>
                              member.status === "accepted" || member.status === "external-closed",
                          ))
                          ? current.verificationAttempt + 1
                          : current.verificationAttempt,
                    },
              );
              yield* recordCommand(kind, input, current.epicRunId);
              if (
                (kind === "resume" &&
                  modeState.mode !== "armed" &&
                  modeState.mode !== "run-once") ||
                (kind === "stop" && (modeState.mode === "armed" || modeState.mode === "run-once"))
              )
                yield* modeIntent(
                  updated,
                  input.commandId,
                  modeState.revision,
                  kind === "resume" ? "armed" : "observe",
                );
              return updated;
            }),
          );
          yield* publish(input.projectId);
          yield* recoverModeIntent(updated);
          return updated;
        }),
      )
      .pipe(Effect.mapError(mapError));

  const processProject = (projectId: ProjectId) =>
    locks
      .withPermit(
        projectId,
        Effect.gen(function* () {
          const beforeQueue = yield* loadEnabledEpicQueue(sql, projectId);
          yield* queue.process(projectId, (epicNumber) => preview({ projectId, epicNumber }));
          const afterQueue = yield* loadEnabledEpicQueue(sql, projectId);
          if (beforeQueue?.revision !== afterQueue?.revision) yield* publish(projectId);
          let state: AgentControlEpicRuntimeView | null = yield* get(projectId);
          if (state) yield* recoverModeIntent(state);
          if (
            !state ||
            state.status === "succeeded" ||
            state.status === "stopped" ||
            state.status === "blocked"
          )
            return;
          if (
            state.dependencyPlan &&
            state.dependencyPlanDigest !== epicDigest(state.dependencyPlan)
          )
            return yield* epicError(
              "authority-conflict",
              "The approved dependency plan changed during execution.",
            );
          const project = yield* engine.getProjectState({ projectId });
          if (project.mode !== "armed" || project.pausedFromMode !== null) return;
          const inspected = yield* inspect({
            projectId,
            epicNumber: state.source.epic.number,
          }).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                const error = mapError(cause);
                if (
                  ["epic-unavailable", "authority-conflict", "revision-conflict"].includes(
                    error.code,
                  )
                )
                  return yield* error;
                yield* block(state!, [
                  { code: error.code, issueNumber: null, message: error.message },
                ]);
                return null;
              }),
            ),
          );
          if (!inspected) return;
          const changes = epicSourceChanges(state, inspected);
          if (changes.length) {
            yield* block(state, changes);
            return;
          }
          const availableTasks = yield* sourceTasks(projectId).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                const error = mapError(cause);
                if (error.code === "intake-incomplete") return null;
                if (
                  ["epic-unavailable", "authority-conflict", "revision-conflict"].includes(
                    error.code,
                  )
                )
                  return yield* error;
                yield* block(state!, [
                  { code: error.code, issueNumber: null, message: error.message },
                ]);
                return null;
              }),
            ),
          );
          if (!availableTasks) return;
          const activeMembers = state.members.filter((member) => member.status === "running");
          for (const active of activeMembers) {
            if (!active.childRunId) continue;
            const currentTask = availableTasks.find((task) => task.taskId === active.taskId);
            if (!currentTask || currentTask.sourceGate !== "eligible") {
              yield* block(state, [
                {
                  code: "task-not-approved",
                  issueNumber: active.issueNumber,
                  message:
                    "The active task lost its current source approval. Restore approval and resume, or stop this Epic. Its result and worktree are retained.",
                },
              ]);
              return;
            }
            const terminal: ReadonlyArray<{
              status: string;
              evidenceId: string;
              reservationId: string | null;
            }> = yield* sql<{
              status: string;
              evidenceId: string;
              reservationId: string | null;
            }>`
        SELECT json_extract(event.payload_json,'$.status') AS status,evidence.task_finalization_evidence_id AS "evidenceId",COALESCE(run.worktree_reservation_id,execution.worktree_reservation_id) AS "reservationId"
        FROM agent_control_task_verification_finalization_evidence evidence
        JOIN agent_control_task_verification_finalization_receipts receipt ON receipt.task_finalization_evidence_id=evidence.task_finalization_evidence_id AND receipt.status='accepted'
        JOIN agent_control_task_verification_finalization_markers marker ON marker.task_finalization_evidence_id=evidence.task_finalization_evidence_id AND marker.receipt_id=receipt.receipt_id
        JOIN agent_control_events event ON event.event_id=evidence.task_event_id
        LEFT JOIN agent_control_run_once_states run ON run.run_id=${active.childRunId} AND run.task_id=evidence.task_id AND run.project_id=evidence.project_id
        LEFT JOIN ${state.dependencyPlan ? sql`agent_control_epic_task_executions` : sql`(SELECT run_id AS execution_id,task_id,project_id,worktree_reservation_id FROM agent_control_run_once_states)`} execution ON execution.execution_id=${active.childRunId} AND execution.task_id=evidence.task_id AND execution.project_id=evidence.project_id
        WHERE evidence.project_id=${projectId} AND evidence.task_id=${active.taskId}
        AND (run.run_id IS NOT NULL OR execution.execution_id IS NOT NULL)`;
            if (!terminal.length) {
              const attention = yield* sql<{
                status: string;
              }>`SELECT status FROM main.agent_control_task_states WHERE task_id=${active.taskId} AND project_id=${projectId}`;
              if (attention[0]?.status === "needs-attention" && state.dependencyPlan) {
                state = yield* persist(state, {
                  members: state.members.map((member) =>
                    member.issueNodeId === active.issueNodeId
                      ? {
                          ...member,
                          status: "failed",
                          waitReason: "blocker",
                          blocker: "Task requires attention; its dependents cannot start.",
                        }
                      : member,
                  ),
                });
                continue;
              }
              if (attention[0]?.status === "needs-attention")
                yield* block(state, [
                  {
                    code: "child-needs-attention",
                    issueNumber: active.issueNumber,
                    message:
                      "The active child requires human attention. Inspect its thread and evidence, then resume this Epic or stop it.",
                  },
                ]);
              if (state.dependencyPlan) continue;
              return;
            }
            if (terminal.length !== 1)
              return yield* epicError(
                "authority-conflict",
                "Epic child finalization is ambiguous.",
              );
            const result = terminal[0]!;
            if (result.status !== "succeeded" || !result.reservationId) {
              state = yield* persist(state, {
                members: state.members.map((member) =>
                  member.issueNodeId === active.issueNodeId
                    ? { ...member, status: "failed", taskFinalizationEvidenceId: result.evidenceId }
                    : member,
                ),
              });
              if (state.dependencyPlan) continue;
              yield* block(state, [
                {
                  code: "child-failed",
                  issueNumber: active.issueNumber,
                  message:
                    "The child task failed or exhausted its bounded repair. Its changes were not accepted. Inspect its thread and evidence. For a queued run, disarm, remove waiting entries and leave the queue to return to ordinary tasks.",
                },
              ]);
              return;
            }
            if (state.dependencyPlan && active.waitReason !== "integration")
              state = yield* persist(state, {
                members: state.members.map((member) =>
                  member.issueNodeId === active.issueNodeId
                    ? { ...member, waitReason: "integration" }
                    : member,
                ),
              });
            const captured: AgentControlEpicAcceptedResult | null = yield* results
              .capture({
                epicRunId: state.epicRunId,
                projectId,
                taskId: active.taskId!,
                childRunId: active.childRunId,
                reservationId: result.reservationId,
                previousCommitSha: state.dependencyPlan
                  ? active.baseCommitSha
                  : (state.acceptedCommitSha ?? state.initialBase?.commitSha ?? null),
                taskFinalizationEvidenceId: result.evidenceId,
              })
              .pipe(
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    if (
                      ["epic-unavailable", "authority-conflict", "revision-conflict"].includes(
                        error.code,
                      )
                    )
                      return yield* error;
                    if (state!.dependencyPlan) {
                      state = yield* persist(state!, {
                        members: state!.members.map((member) =>
                          member.issueNodeId === active.issueNodeId
                            ? {
                                ...member,
                                status: "failed",
                                waitReason: "blocker",
                                blocker: error.message,
                              }
                            : member,
                        ),
                        blockers: [
                          ...state!.blockers,
                          {
                            code: error.code,
                            issueNumber: active.issueNumber,
                            message: error.message,
                          },
                        ],
                      });
                    } else
                      yield* block(state!, [
                        {
                          code: error.code,
                          issueNumber: active.issueNumber,
                          message: error.message,
                        },
                      ]);
                    return null;
                  }),
                ),
              );
            if (!captured) {
              if (state.dependencyPlan) continue;
              return;
            }
            let accepted: AgentControlEpicAcceptedResult = captured;
            let integrationVerification: AgentControlEpicFinalVerification | undefined =
              state.integrationVerification;
            if (state.dependencyPlan) {
              if (!results.integrate || !state.initialBase)
                return yield* epicError(
                  "integration-unavailable",
                  "Planned execution requires integrated result verification.",
                );
              const expectedState: AgentControlEpicRuntimeView = state;
              const member: AgentControlEpicMemberView = {
                ...active,
                captured,
                accepted: captured,
                reservationId: result.reservationId,
                taskFinalizationEvidenceId: result.evidenceId,
              };
              const integrated: {
                accepted: AgentControlEpicAcceptedResult;
                verification: AgentControlEpicFinalVerification;
              } | null = yield* results
                .integrate({
                  epicRunId: state.epicRunId,
                  projectId,
                  commitSha: captured.commitSha,
                  captured,
                  expectedCommitSha: state.acceptedCommitSha ?? state.initialBase.commitSha,
                  initialBaseCommitSha: state.initialBase.commitSha,
                  firstAccepted: state.members.find((item) => item.accepted) ?? member,
                  lastAccepted: member,
                  checks: state.checks,
                  attempt: state.verificationAttempt,
                  refreshSource: Effect.gen(function* () {
                    const observed = yield* inspect({
                      projectId,
                      epicNumber: expectedState.source.epic.number,
                    });
                    const changes = epicSourceChanges(expectedState, observed);
                    if (changes.length)
                      return yield* epicError(
                        "scope-changed",
                        changes.map((change) => change.message).join(" "),
                      );
                  }).pipe(Effect.mapError(mapError)),
                  authorize: Effect.gen(function* () {
                    const selected = yield* get(projectId);
                    yield* requireEpicIntegrationAuthority(expectedState, selected);
                  }).pipe(Effect.mapError(mapError)),
                  authorizePublication: Effect.gen(function* () {
                    const project = yield* engine.getProjectState({ projectId });
                    const current = (yield* sourceTasks(projectId)).find(
                      (task) => task.taskId === active.taskId,
                    );
                    const snapshot = yield* github.getCompletedSnapshot(projectId);
                    if (
                      Option.isNone(snapshot) ||
                      (current &&
                        current.sequence !== snapshot.value.sourcePrecondition.githubIntakeSequence)
                    )
                      return yield* epicError(
                        "intake-incomplete",
                        "Wait for current GitHub reconciliation before accepting integration.",
                      );
                    const issue = snapshot.value.issues.find(
                      (issue) => issue.issueNodeId === active.issueNodeId,
                    );
                    const frozen = expectedState.source.tasks.find(
                      (task) => task.issue.issueNodeId === active.issueNodeId,
                    )?.issue;
                    if (
                      project.mode !== "armed" ||
                      project.pausedFromMode !== null ||
                      !current ||
                      current.sourceGate !== "eligible" ||
                      !issue ||
                      issue.state !== "open" ||
                      !issue.ready ||
                      issue.paused ||
                      !issue.eligible ||
                      !issue.timelineComplete ||
                      issue.eligibilityReason !== "eligible" ||
                      !frozen ||
                      issue.repositoryNodeId !== frozen.repositoryNodeId ||
                      (frozen.contentFingerprint !== undefined &&
                        epicIssueContentFingerprint(issue) !== frozen.contentFingerprint)
                    )
                      return yield* epicError(
                        "task-not-approved",
                        "Task source approval or content changed during integration. Restore the approved source and resume, or stop this Epic. The captured result is retained.",
                      );
                  }).pipe(Effect.mapError(mapError)),
                })
                .pipe(
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      if (error.code === "intake-incomplete") return null;
                      if (
                        ["epic-unavailable", "authority-conflict", "revision-conflict"].includes(
                          error.code,
                        )
                      )
                        return yield* error;
                      state = yield* persist(state!, {
                        members: state!.members.map((item) =>
                          item.issueNodeId === active.issueNodeId
                            ? {
                                ...item,
                                captured,
                                reservationId: result.reservationId,
                                taskFinalizationEvidenceId: result.evidenceId,
                                waitReason: "blocker",
                                blocker: error.message,
                                status: "failed",
                              }
                            : item,
                        ),
                        blockers: [
                          ...state!.blockers,
                          {
                            code: error.code,
                            issueNumber: active.issueNumber,
                            message: error.message,
                          },
                        ],
                      });
                      return null;
                    }),
                  ),
                );
              if (!integrated) continue;
              if (integrated.verification.status !== "passed") {
                state = yield* persist(state, {
                  members: state.members.map((item) =>
                    item.issueNodeId === active.issueNodeId
                      ? {
                          ...item,
                          captured,
                          reservationId: result.reservationId,
                          taskFinalizationEvidenceId: result.evidenceId,
                          integrationVerification: integrated.verification,
                          status: "failed",
                          waitReason: "blocker",
                          blocker: integrated.verification.detail,
                        }
                      : item,
                  ),
                  blockers: [
                    ...state.blockers,
                    {
                      code: "integration-check-failed",
                      issueNumber: active.issueNumber,
                      message: integrated.verification.detail,
                    },
                  ],
                });
                continue;
              }
              if (integrated.verification.commitSha !== integrated.accepted.commitSha)
                return yield* epicError(
                  "authority-conflict",
                  "Integration proof belongs to another head.",
                );
              accepted = integrated.accepted;
              integrationVerification = integrated.verification;
            }
            state = yield* persist(state, {
              activeTaskId: null,
              finalVerification: null,
              ...(integrationVerification ? { integrationVerification } : {}),
              acceptedCommitSha: accepted.commitSha,
              members: state.members.map((member) => {
                if (member.issueNodeId !== active.issueNodeId) return member;
                const { blocker: _blocker, waitReason: _waitReason, ...retained } = member;
                return {
                  ...retained,
                  status: "accepted",
                  accepted,
                  ...(state!.dependencyPlan
                    ? { captured, ...(integrationVerification ? { integrationVerification } : {}) }
                    : {}),
                  reservationId: result.reservationId,
                  taskFinalizationEvidenceId: result.evidenceId,
                };
              }),
            });
          }
          const externalPrerequisites = [...(state.externalPrerequisites ?? [])];
          for (const task of inspected.tasks) {
            for (const dependency of task.dependencies) {
              if (
                dependency.state === "closed" &&
                !state.members.some((member) => member.issueNodeId === dependency.issueNodeId) &&
                !externalPrerequisites.some((item) => item.issueNodeId === dependency.issueNodeId)
              ) {
                externalPrerequisites.push({
                  issueNodeId: dependency.issueNodeId,
                  issueNumber: dependency.number,
                  observedAt: inspected.inspectedAt,
                });
              }
            }
          }
          if (externalPrerequisites.length !== (state.externalPrerequisites?.length ?? 0))
            state = yield* persist(state, { externalPrerequisites });
          if (!state.dependencyPlan && state.members.some((member) => member.status === "failed")) {
            yield* block(state, [
              {
                code: "child-failed",
                issueNumber: null,
                message:
                  "A failed child requires inspection. For a queued run, disarm, remove waiting entries and leave the queue before starting a new scope.",
              },
            ]);
            return;
          }
          if (!state.dependencyPlan && state.activeTaskId !== null) {
            const selected = availableTasks.find((task) => task.taskId === state!.activeTaskId);
            if (
              !selected ||
              selected.status !== "candidate" ||
              selected.sourceGate !== "eligible" ||
              selected.stage !== "intake"
            )
              yield* block(state, [
                {
                  code: selected ? "task-not-approved" : "missing-issue",
                  issueNumber:
                    state.members.find((member) => member.taskId === state!.activeTaskId)
                      ?.issueNumber ?? null,
                  message:
                    "The selected child lost its complete, trusted intake authority before starting. Restore its approval and resume, or stop this Epic.",
                },
              ]);
            return;
          }
          const eligibleIssueIds = new Set(
            availableTasks
              .filter(
                (task) =>
                  task.status === "candidate" &&
                  task.sourceGate === "eligible" &&
                  task.stage === "intake",
              )
              .map((task) => task.issueNodeId),
          );
          if (state.dependencyPlan) {
            let members = state.members;
            const selectedState = { ...state };
            while (
              members.filter((member) => member.status === "running").length <
              (state.parallelism ?? 1)
            ) {
              const next = selectEpicMember(selectedState, eligibleIssueIds);
              if (!next) break;
              const task = availableTasks.find(
                (item) => item.issueNodeId === next.issue.issueNodeId,
              )!;
              members = members.map((member) =>
                member.issueNodeId === next.issue.issueNodeId
                  ? {
                      ...member,
                      taskId: AgentControlTaskId.make(task.taskId),
                      status: "running" as const,
                      waitReason: "capacity" as const,
                      baseCommitSha: state!.acceptedCommitSha ?? state!.initialBase!.commitSha,
                    }
                  : member,
              );
              selectedState.members = members;
            }
            members = members.map((member) =>
              member.status === "pending"
                ? {
                    ...member,
                    waitReason: epicDependenciesSatisfied(selectedState, member.issueNodeId)
                      ? ("capacity" as const)
                      : ("dependencies" as const),
                  }
                : member,
            );
            if (epicDigest(members) !== epicDigest(state.members))
              state = yield* persist(state, {
                members,
                activeTaskId:
                  members.find(
                    (member) => member.status === "running" && member.childRunId === null,
                  )?.taskId ?? null,
              });
            if (members.some((member) => member.status === "running")) return;
            if (members.some((member) => member.status === "failed")) {
              yield* block(
                state,
                state.blockers.length
                  ? state.blockers
                  : [
                      {
                        code: "child-failed",
                        issueNumber: null,
                        message:
                          "Failed tasks block their dependents. Independent work has drained; inspect each task blocker.",
                      },
                    ],
              );
              return;
            }
          }
          const next = selectEpicMember(state, eligibleIssueIds);
          if (!next) {
            if (state.members.some((member) => member.status === "pending")) {
              yield* block(state, [
                {
                  code: "dependencies-blocked",
                  issueNumber: null,
                  message:
                    "No remaining task has current trusted approval and all explicit prerequisites satisfied. Check missing issues, approvals, prerequisites, and dependency cycles.",
                },
              ]);
              return;
            }
            const lastAccepted = state.members
              .toReversed()
              .find((member) => member.accepted?.commitSha === state!.acceptedCommitSha);
            const firstAccepted = state.members.find(
              (member) =>
                member.accepted && member.baseCommitSha === (state!.initialBase?.commitSha ?? null),
            );
            if (!firstAccepted || !lastAccepted || !state.acceptedCommitSha) {
              yield* block(state, [
                {
                  code: "no-accepted-result",
                  issueNumber: null,
                  message:
                    "The original accepted member or final result is unavailable; T3 cannot verify the complete Epic.",
                },
              ]);
              return;
            }
            if (state.status !== "verifying")
              state = yield* persist(state, { status: "verifying" });
            const finalVerification = yield* results
              .verify({
                epicRunId: state.epicRunId,
                projectId,
                commitSha: state.acceptedCommitSha!,
                initialBaseCommitSha: state.initialBase?.commitSha ?? null,
                firstAccepted,
                checks: state.checks,
                lastAccepted,
                attempt: state.verificationAttempt,
              })
              .pipe(
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    if (
                      ["epic-unavailable", "authority-conflict", "revision-conflict"].includes(
                        error.code,
                      )
                    )
                      return yield* error;
                    yield* block(state!, [
                      { code: error.code, issueNumber: null, message: error.message },
                    ]);
                    return null;
                  }),
                ),
              );
            if (!finalVerification) return;
            if (finalVerification.commitSha !== state.acceptedCommitSha)
              return yield* epicError(
                "authority-conflict",
                "Epic verification checked a different result commit.",
              );
            state = yield* persist(state, {
              finalVerification,
              finalVerificationHistory: [...state.finalVerificationHistory, finalVerification],
              status: finalVerification.status === "passed" ? "succeeded" : "verifying",
            });
            if (finalVerification.status !== "passed")
              yield* block(state, [
                {
                  code: "final-verification-failed",
                  issueNumber: null,
                  message: finalVerification.detail,
                },
              ]);
            return;
          }
          const task = availableTasks.find((item) => item.issueNodeId === next.issue.issueNodeId)!;
          const taskId = AgentControlTaskId.make(task.taskId);
          yield* persist(state, {
            activeTaskId: taskId,
            members: state.members.map((member) =>
              member.issueNodeId === next.issue.issueNodeId
                ? {
                    ...member,
                    taskId,
                    status: "running",
                    baseCommitSha:
                      state!.acceptedCommitSha ?? state!.initialBase?.commitSha ?? null,
                  }
                : member,
            ),
          });
        }),
      )
      .pipe(Effect.mapError(mapError));

  const handoffEvidence = yield* Effect.serviceOption(EpicHandoffEvidence);
  const handoffRemote = yield* Effect.serviceOption(EpicHandoffRemote);
  const unavailableHandoff = () =>
    Effect.fail(
      epicError("handoff-unavailable", "Epic publication is unavailable on this server."),
    );
  const handoff =
    Option.isSome(handoffEvidence) && Option.isSome(handoffRemote)
      ? yield* makeEpicHandoff({ onChange: publish, withProjectLock: locks.withPermit }).pipe(
          Effect.provideService(EpicHandoffEvidence, handoffEvidence.value),
          Effect.provideService(EpicHandoffRemote, handoffRemote.value),
        )
      : {
          previewHandoff: unavailableHandoff,
          publishHandoff: unavailableHandoff,
          recoverPending: () => Effect.void,
        };
  yield* handoff.recoverPending().pipe(
    Effect.catch(() =>
      Effect.logWarning("Epic publication recovery could not load pending handoffs."),
    ),
    Effect.forkScoped,
  );

  return AgentControlEpic.of({
    changeQueue: (input: AgentControlEpicQueueChangeInput) =>
      locks
        .withPermit(
          input.projectId,
          queue.change(input, (epicNumber) => preview({ projectId: input.projectId, epicNumber })),
        )
        .pipe(
          Effect.tap(() => publish(input.projectId)),
          Effect.mapError(mapEpicQueueError),
        ),
    subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
    get,
    preview,
    previewHandoff: handoff.previewHandoff,
    publishHandoff: handoff.publishHandoff,
    start,
    resume: (input) => control("resume", input),
    stop: (input) => control("stop", input),
    clear: (input) => control("clear", input),
    processProject,
  });
});

export const AgentControlEpicLive = Layer.effect(AgentControlEpic, makeAgentControlEpic);
