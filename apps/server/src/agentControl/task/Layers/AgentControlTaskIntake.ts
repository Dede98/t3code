import {
  AgentControlTaskGetInput,
  AgentControlTaskListInput,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskCommand,
  type AgentControlTaskSourceGate,
  type AgentControlTaskSourceSnapshot,
  type AgentControlTaskState,
  type AgentControlTaskSummary,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { deriveAgentControlTaskCommandId, deriveAgentControlTaskId } from "../identity.ts";
import {
  AgentControlTaskIntake,
  type AgentControlTaskIntakeShape,
} from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlTaskGetInput);
const decodeList = Schema.decodeUnknownEffect(AgentControlTaskListInput);
const decodeReconcile = Schema.decodeUnknownEffect(AgentControlTaskReconcileOnceInput);

const safeError = (
  code: AgentControlTaskRpcError["code"],
  operation: AgentControlTaskRpcError["operation"],
  projectId: AgentControlTaskGetInput["projectId"],
  taskId: AgentControlTaskGetInput["taskId"] | null = null,
) => new AgentControlTaskRpcError({ code, operation, projectId, taskId });

const ensureProject = Effect.fn("AgentControlTaskIntake.ensureProject")(function* (
  availability: AgentControlProjectAvailability["Service"],
  projectId: AgentControlTaskGetInput["projectId"],
  operation: AgentControlTaskRpcError["operation"],
) {
  const result = yield* Effect.result(availability.ensureAvailable(projectId));
  if (result._tag === "Success") return;
  if (result.failure._tag === "AgentControlProjectUnavailableError") {
    return yield* safeError(
      result.failure.reason === "missing" ? "project-missing" : "project-deleted",
      operation,
      projectId,
    );
  }
  return yield* safeError("internal-persistence-error", operation, projectId);
});

const sourceGate = (issue: AgentControlGithubIssueSnapshot): AgentControlTaskSourceGate => {
  if (!issue.timelineComplete || issue.eligibilityReason === "timeline-invalid") {
    return "timeline-invalid";
  }
  if (issue.state === "closed" || issue.eligibilityReason === "closed") return "closed";
  if (issue.paused || issue.eligibilityReason === "paused") return "paused";
  return issue.eligible && issue.ready ? "eligible" : "not-ready";
};

const sourceSnapshot = (
  issue: AgentControlGithubIssueSnapshot,
): AgentControlTaskSourceSnapshot => ({
  repositoryNodeId: issue.repositoryNodeId,
  issueNodeId: issue.issueNodeId,
  number: issue.number,
  url: issue.url,
  state: issue.state,
  title: issue.title,
  body: issue.body,
  contentTrust: "untrusted-external",
  updatedAt: issue.updatedAt,
  timelineComplete: issue.timelineComplete,
  ready: issue.ready,
  paused: issue.paused,
  eligible: issue.eligible,
  eligibilityReason: issue.eligibilityReason,
});

const sameSourceSnapshot = (
  left: AgentControlTaskSourceSnapshot,
  right: AgentControlTaskSourceSnapshot,
) =>
  left.repositoryNodeId === right.repositoryNodeId &&
  left.issueNodeId === right.issueNodeId &&
  left.number === right.number &&
  left.url === right.url &&
  left.state === right.state &&
  left.title === right.title &&
  left.body === right.body &&
  left.contentTrust === right.contentTrust &&
  left.updatedAt === right.updatedAt &&
  left.timelineComplete === right.timelineComplete &&
  left.ready === right.ready &&
  left.paused === right.paused &&
  left.eligible === right.eligible &&
  left.eligibilityReason === right.eligibilityReason;

const summary = (state: AgentControlTaskState): AgentControlTaskSummary => ({
  schemaVersion: 1,
  taskId: state.taskId,
  source: state.source,
  status: state.status,
  sourceGate: state.sourceGate,
  stage: state.stage,
  sourceUpdatedAt: state.sourceUpdatedAt,
  githubIntakeSequence: state.githubIntakeSequence,
  title: state.sourceSnapshot.title,
  contentTrust: "untrusted-external",
  createdAt: state.createdAt,
  updatedAt: state.updatedAt,
  revision: state.revision,
  sequence: state.sequence,
});

const makeIntake = Effect.gen(function* () {
  const availability = yield* AgentControlProjectAvailability;
  const github = yield* AgentControlGithubStateRepository;
  const engine = yield* AgentControlTaskEngine;
  const states = yield* AgentControlTaskStateRepository;

  const getTask: AgentControlTaskIntakeShape["getTask"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError("validation", "get-task", rawInput.projectId, rawInput.taskId),
        ),
      );
      yield* ensureProject(availability, input.projectId, "get-task");
      const state = yield* engine
        .get(input.taskId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "get-task", input.projectId, input.taskId),
          ),
        );
      if (Option.isNone(state) || state.value.source.projectId !== input.projectId) {
        return yield* safeError("task-missing", "get-task", input.projectId, input.taskId);
      }
      return state.value;
    });

  const listTasks: AgentControlTaskIntakeShape["listTasks"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list-tasks", rawInput.projectId)),
      );
      yield* ensureProject(availability, input.projectId, "list-tasks");
      const entries = yield* states
        .listProject(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "list-tasks", input.projectId),
          ),
        );
      return {
        projectId: input.projectId,
        tasks: entries.flatMap((entry) => (entry._tag === "Valid" ? [summary(entry.state)] : [])),
        quarantinedCount: entries.filter((entry) => entry._tag === "Corrupt").length,
      };
    });

  const reconcileOnce: AgentControlTaskIntakeShape["reconcileOnce"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeReconcile(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "reconcile-once", rawInput.projectId)),
      );
      yield* ensureProject(availability, input.projectId, "reconcile-once");
      const githubStateOption = yield* github
        .get(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "reconcile-once", input.projectId),
          ),
        );
      if (Option.isNone(githubStateOption)) {
        return yield* safeError("source-snapshot-unavailable", "reconcile-once", input.projectId);
      }
      const githubState = githubStateOption.value;
      if (
        githubState.config === null ||
        githubState.pollStatus.status !== "success" ||
        githubState.sequence <= 0
      ) {
        return yield* safeError("source-snapshot-unavailable", "reconcile-once", input.projectId);
      }
      const issues = yield* github
        .listIssues(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "reconcile-once", input.projectId),
          ),
        );
      const entries = yield* states
        .listProject(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "reconcile-once", input.projectId),
          ),
        );
      const existing = entries.flatMap((entry) => (entry._tag === "Valid" ? [entry.state] : []));
      const existingByIdentity = new Map(
        existing.map((task) => [
          `${task.source.repositoryNodeId}\u0000${task.source.issueNodeId}`,
          task,
        ]),
      );
      const existingByNumber = new Map<number, AgentControlTaskState>();
      const existingByIssueNode = new Map<string, AgentControlTaskState>();
      for (const task of existing) {
        existingByNumber.set(task.source.issueNumber, task);
        existingByIssueNode.set(task.source.issueNodeId, task);
      }

      let createdCount = 0;
      let updatedCount = 0;
      let needsAttentionCount = 0;
      let unchangedCount = 0;
      const observedIdentities = new Set<string>();
      let snapshotIdentityValid = true;

      for (const issue of issues) {
        const identityKey = `${issue.repositoryNodeId}\u0000${issue.issueNodeId}`;
        const repositoryValid =
          issue.repositoryNodeId === githubState.config.repository.repositoryNodeId;
        const exact = existingByIdentity.get(identityKey);
        const numberConflict = existingByNumber.get(issue.number);
        const transferConflict = existingByIssueNode.get(issue.issueNodeId);
        const exactMetadataValid =
          exact === undefined ||
          (exact.source.issueNumber === issue.number &&
            exact.source.issueUrl === issue.url &&
            exact.source.repositoryNodeId === issue.repositoryNodeId &&
            exact.source.issueNodeId === issue.issueNodeId);
        const conflict =
          exact ??
          (numberConflict !== undefined &&
          (numberConflict.source.issueNodeId !== issue.issueNodeId ||
            numberConflict.source.repositoryNodeId !== issue.repositoryNodeId)
            ? numberConflict
            : undefined) ??
          (transferConflict !== undefined &&
          transferConflict.source.repositoryNodeId !== issue.repositoryNodeId
            ? transferConflict
            : undefined);

        if (
          !repositoryValid ||
          !exactMetadataValid ||
          (conflict !== undefined && conflict !== exact)
        ) {
          snapshotIdentityValid = false;
          if (conflict !== undefined) {
            if (
              conflict.sourceGate === "identity-invalid" &&
              conflict.githubIntakeSequence === githubState.sequence
            ) {
              unchangedCount += 1;
              continue;
            }
            const markNeedsAttention =
              conflict.status === "candidate" || conflict.status === "needs-attention";
            const commandId = yield* deriveAgentControlTaskCommandId([
              markNeedsAttention
                ? "agentControl.task.markNeedsAttention"
                : "agentControl.task.sourceGate.refresh",
              conflict.taskId,
              String(conflict.revision),
              "identity-invalid",
              String(githubState.sequence),
            ]);
            const command: AgentControlTaskCommand = markNeedsAttention
              ? {
                  type: "agentControl.task.markNeedsAttention",
                  commandId,
                  taskId: conflict.taskId,
                  projectId: input.projectId,
                  expectedRevision: conflict.revision,
                  sourceGate: "identity-invalid",
                  sourceUpdatedAt: conflict.sourceUpdatedAt,
                  githubIntakeSequence: githubState.sequence,
                }
              : {
                  type: "agentControl.task.sourceGate.refresh",
                  commandId,
                  taskId: conflict.taskId,
                  projectId: input.projectId,
                  expectedRevision: conflict.revision,
                  source: conflict.source,
                  sourceGate: "identity-invalid",
                  sourceUpdatedAt: conflict.sourceUpdatedAt,
                  githubIntakeSequence: githubState.sequence,
                  sourceSnapshot: conflict.sourceSnapshot,
                };
            const result = yield* engine.dispatchController(command);
            if (result.eventCreated) {
              if (markNeedsAttention) needsAttentionCount += 1;
              else updatedCount += 1;
            } else {
              unchangedCount += 1;
            }
          }
          continue;
        }

        observedIdentities.add(identityKey);
        const gate = sourceGate(issue);
        const snapshot = sourceSnapshot(issue);
        if (exact === undefined) {
          if (gate !== "eligible") {
            unchangedCount += 1;
            continue;
          }
          const source = {
            projectId: input.projectId,
            repositoryNodeId: issue.repositoryNodeId,
            issueNodeId: issue.issueNodeId,
            issueNumber: issue.number,
            issueUrl: issue.url,
          } as const;
          const taskId = yield* deriveAgentControlTaskId(source);
          const commandId = yield* deriveAgentControlTaskCommandId([
            "agentControl.task.createFromGithubIssue",
            taskId,
            issue.updatedAt,
            gate,
            String(githubState.sequence),
          ]);
          const result = yield* engine.dispatchController({
            type: "agentControl.task.createFromGithubIssue",
            commandId,
            taskId,
            projectId: input.projectId,
            expectedRevision: 0,
            source,
            sourceGate: gate,
            sourceUpdatedAt: issue.updatedAt,
            githubIntakeSequence: githubState.sequence,
            sourceSnapshot: snapshot,
          });
          if (result.eventCreated) createdCount += 1;
          else unchangedCount += 1;
          existingByIdentity.set(identityKey, result.state);
          existingByNumber.set(issue.number, result.state);
          existingByIssueNode.set(issue.issueNodeId, result.state);
          continue;
        }

        if (
          exact.sourceGate === gate &&
          exact.sourceUpdatedAt === issue.updatedAt &&
          exact.githubIntakeSequence === githubState.sequence &&
          sameSourceSnapshot(exact.sourceSnapshot, snapshot)
        ) {
          unchangedCount += 1;
          continue;
        }
        const commandId = yield* deriveAgentControlTaskCommandId([
          "agentControl.task.sourceGate.refresh",
          exact.taskId,
          String(exact.revision),
          issue.updatedAt,
          gate,
          String(githubState.sequence),
        ]);
        const result = yield* engine.dispatchController({
          type: "agentControl.task.sourceGate.refresh",
          commandId,
          taskId: exact.taskId,
          projectId: input.projectId,
          expectedRevision: exact.revision,
          source: exact.source,
          sourceGate: gate,
          sourceUpdatedAt: issue.updatedAt,
          githubIntakeSequence: githubState.sequence,
          sourceSnapshot: snapshot,
        });
        if (result.eventCreated) updatedCount += 1;
        else unchangedCount += 1;
      }

      if (snapshotIdentityValid) {
        for (const task of existing) {
          const key = `${task.source.repositoryNodeId}\u0000${task.source.issueNodeId}`;
          if (observedIdentities.has(key)) continue;
          if (
            task.sourceGate === "source-missing" &&
            task.status === "needs-attention" &&
            task.githubIntakeSequence === githubState.sequence
          ) {
            unchangedCount += 1;
            continue;
          }
          const commandId = yield* deriveAgentControlTaskCommandId([
            task.status === "candidate"
              ? "agentControl.task.markNeedsAttention"
              : "agentControl.task.sourceGate.refresh",
            task.taskId,
            String(task.revision),
            "source-missing",
            String(githubState.sequence),
          ]);
          const command: AgentControlTaskCommand =
            task.status === "candidate"
              ? {
                  type: "agentControl.task.markNeedsAttention",
                  commandId,
                  taskId: task.taskId,
                  projectId: input.projectId,
                  expectedRevision: task.revision,
                  sourceGate: "source-missing",
                  sourceUpdatedAt: task.sourceUpdatedAt,
                  githubIntakeSequence: githubState.sequence,
                }
              : {
                  type: "agentControl.task.sourceGate.refresh",
                  commandId,
                  taskId: task.taskId,
                  projectId: input.projectId,
                  expectedRevision: task.revision,
                  source: task.source,
                  sourceGate: "source-missing",
                  sourceUpdatedAt: task.sourceUpdatedAt,
                  githubIntakeSequence: githubState.sequence,
                  sourceSnapshot: task.sourceSnapshot,
                };
          const result = yield* engine.dispatchController(command);
          if (result.eventCreated) {
            if (task.status === "candidate") needsAttentionCount += 1;
            else updatedCount += 1;
          } else {
            unchangedCount += 1;
          }
        }
      }

      return {
        projectId: input.projectId,
        githubIntakeSequence: githubState.sequence,
        observedCount: issues.length,
        createdCount,
        updatedCount,
        needsAttentionCount,
        unchangedCount,
      };
    });

  return AgentControlTaskIntake.of({ getTask, listTasks, reconcileOnce });
});

export const layer = Layer.effect(AgentControlTaskIntake, makeIntake);
