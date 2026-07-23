import {
  AgentControlTaskGetInput,
  AgentControlTaskListInput,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskReconcileOnceResult,
  type AgentControlTaskSourceGate,
  type AgentControlTaskSourceSnapshot,
  type AgentControlTaskState,
  type AgentControlTaskSummary,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { deriveAgentControlTaskCommandId, deriveAgentControlTaskId } from "../identity.ts";
import {
  AgentControlTaskIntake,
  type AgentControlTaskIntakeShape,
} from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const MAX_RECONCILE_ATTEMPTS = 3;
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
  if (issue.state === "closed" || issue.eligibilityReason === "closed") {
    return "closed";
  }
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

const identityKey = (repositoryNodeId: string, issueNodeId: string) =>
  `${repositoryNodeId}\u0000${issueNodeId}`;
const numberKey = (repositoryNodeId: string, issueNumber: number) =>
  `${repositoryNodeId}\u0000${issueNumber}`;

const makeIntake = Effect.gen(function* () {
  const availability = yield* AgentControlProjectAvailability;
  const github = yield* AgentControlGithubStateRepository;
  const engine = yield* AgentControlTaskEngine;
  const states = yield* AgentControlTaskStateRepository;
  const reconciles = yield* AgentControlTaskReconcileStateRepository;
  const locks = yield* SynchronizedRef.make(new Map<ProjectId, Semaphore.Semaphore>());

  const getProjectLock = (projectId: ProjectId) =>
    SynchronizedRef.modifyEffect(locks, (current) => {
      const existing = current.get(projectId);
      if (existing !== undefined) {
        return Effect.succeed([existing, current] as const);
      }
      return Semaphore.make(1).pipe(
        Effect.map((lock) => {
          const next = new Map(current);
          next.set(projectId, lock);
          return [lock, next] as const;
        }),
      );
    });

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

  const loadPassSnapshot = Effect.fn("AgentControlTaskIntake.loadPassSnapshot")(function* (
    projectId: ProjectId,
  ) {
    yield* ensureProject(availability, projectId, "reconcile-once");
    const source = yield* github
      .getCompletedSnapshot(projectId)
      .pipe(
        Effect.mapError(() => safeError("internal-persistence-error", "reconcile-once", projectId)),
      );
    if (Option.isNone(source)) {
      return yield* safeError("source-snapshot-unavailable", "reconcile-once", projectId);
    }
    const entries = yield* states
      .listProject(projectId)
      .pipe(
        Effect.mapError(() => safeError("internal-persistence-error", "reconcile-once", projectId)),
      );
    if (entries.some((entry) => entry._tag === "Corrupt")) {
      return yield* safeError("task-projection-corrupt", "reconcile-once", projectId);
    }
    const existing = entries.flatMap((entry) => (entry._tag === "Valid" ? [entry.state] : []));
    const identities = new Set<string>();
    const numbers = new Set<string>();
    for (const task of existing) {
      const byIdentity = identityKey(task.source.repositoryNodeId, task.source.issueNodeId);
      const byNumber = numberKey(task.source.repositoryNodeId, task.source.issueNumber);
      if (identities.has(byIdentity) || numbers.has(byNumber)) {
        return yield* safeError("source-identity-conflict", "reconcile-once", projectId);
      }
      identities.add(byIdentity);
      numbers.add(byNumber);
    }
    return {
      sourcePrecondition: source.value.sourcePrecondition,
      issues: source.value.issues,
      existing,
    };
  });

  const runPass = Effect.fn("AgentControlTaskIntake.runPass")(function* (projectId: ProjectId) {
    const pass = yield* loadPassSnapshot(projectId);
    const { sourcePrecondition, issues, existing } = pass;
    const startedAt = DateTime.formatIso(yield* DateTime.now);
    const watermark = yield* reconciles
      .begin(projectId, sourcePrecondition.githubIntakeSequence, startedAt)
      .pipe(
        Effect.mapError((failure) =>
          failure._tag === "AgentControlTaskReconcileConflictError"
            ? safeError("source-snapshot-stale", "reconcile-once", projectId)
            : safeError("internal-persistence-error", "reconcile-once", projectId),
        ),
      );

    const execute = Effect.gen(function* () {
      const existingByIdentity = new Map<string, AgentControlTaskState>();
      const existingByNumber = new Map<string, AgentControlTaskState>();
      const existingByIssueNode = new Map<string, AgentControlTaskState>();
      for (const task of existing) {
        existingByIdentity.set(
          identityKey(task.source.repositoryNodeId, task.source.issueNodeId),
          task,
        );
        existingByNumber.set(
          numberKey(task.source.repositoryNodeId, task.source.issueNumber),
          task,
        );
        existingByIssueNode.set(task.source.issueNodeId, task);
      }

      let createdCount = 0;
      let updatedCount = 0;
      let needsAttentionCount = 0;
      let unchangedCount = 0;
      const observedIdentities = new Set<string>();
      let snapshotIdentityValid = true;

      for (const issue of issues) {
        const issueIdentityKey = identityKey(issue.repositoryNodeId, issue.issueNodeId);
        const repositoryValid = issue.repositoryNodeId === sourcePrecondition.repositoryNodeId;
        const exact = existingByIdentity.get(issueIdentityKey);
        const numberConflict = existingByNumber.get(
          numberKey(issue.repositoryNodeId, issue.number),
        );
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
              conflict.status === "needs-attention"
            ) {
              unchangedCount += 1;
              continue;
            }
            const commandId = yield* deriveAgentControlTaskCommandId([
              "agentControl.task.markNeedsAttention",
              conflict.taskId,
              String(conflict.revision),
              "identity-invalid",
              String(sourcePrecondition.githubIntakeSequence),
            ]);
            const result = yield* engine.dispatchController({
              type: "agentControl.task.markNeedsAttention",
              commandId,
              taskId: conflict.taskId,
              projectId,
              expectedRevision: conflict.revision,
              sourcePrecondition,
              source: conflict.source,
              sourceGate: "identity-invalid",
              sourceUpdatedAt: conflict.sourceUpdatedAt,
              githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
              sourceSnapshot: conflict.sourceSnapshot,
            });
            if (result.eventCreated) needsAttentionCount += 1;
            else unchangedCount += 1;
          }
          continue;
        }

        observedIdentities.add(issueIdentityKey);
        const gate = sourceGate(issue);
        const snapshot = sourceSnapshot(issue);
        if (exact === undefined) {
          if (gate !== "eligible") {
            unchangedCount += 1;
            continue;
          }
          const source = {
            projectId,
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
            String(sourcePrecondition.githubIntakeSequence),
          ]);
          const result = yield* engine.dispatchController({
            type: "agentControl.task.createFromGithubIssue",
            commandId,
            taskId,
            projectId,
            expectedRevision: 0,
            sourcePrecondition,
            source,
            sourceGate: gate,
            sourceUpdatedAt: issue.updatedAt,
            githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
            sourceSnapshot: snapshot,
          });
          if (result.eventCreated) createdCount += 1;
          else unchangedCount += 1;
          existingByIdentity.set(issueIdentityKey, result.state);
          existingByNumber.set(numberKey(issue.repositoryNodeId, issue.number), result.state);
          existingByIssueNode.set(issue.issueNodeId, result.state);
          continue;
        }

        if (exact.status === "needs-attention" && exact.sourceGate === "identity-invalid") {
          unchangedCount += 1;
          continue;
        }
        if (exact.status === "needs-attention" && exact.sourceGate === "source-missing") {
          if (
            gate !== "eligible" ||
            sourcePrecondition.githubIntakeSequence <= exact.githubIntakeSequence
          ) {
            unchangedCount += 1;
            continue;
          }
          const commandId = yield* deriveAgentControlTaskCommandId([
            "agentControl.task.recoverSourceMissing",
            exact.taskId,
            String(exact.revision),
            issue.updatedAt,
            String(sourcePrecondition.githubIntakeSequence),
          ]);
          const result = yield* engine.dispatchController({
            type: "agentControl.task.recoverSourceMissing",
            commandId,
            taskId: exact.taskId,
            projectId,
            expectedRevision: exact.revision,
            sourcePrecondition,
            source: exact.source,
            sourceGate: "eligible",
            sourceUpdatedAt: issue.updatedAt,
            githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
            sourceSnapshot: snapshot,
          });
          if (result.eventCreated) updatedCount += 1;
          else unchangedCount += 1;
          continue;
        }

        if (
          exact.sourceGate === gate &&
          exact.sourceUpdatedAt === issue.updatedAt &&
          exact.githubIntakeSequence === sourcePrecondition.githubIntakeSequence &&
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
          String(sourcePrecondition.githubIntakeSequence),
        ]);
        const result = yield* engine.dispatchController({
          type: "agentControl.task.sourceGate.refresh",
          commandId,
          taskId: exact.taskId,
          projectId,
          expectedRevision: exact.revision,
          sourcePrecondition,
          source: exact.source,
          sourceGate: gate,
          sourceUpdatedAt: issue.updatedAt,
          githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: snapshot,
        });
        if (result.eventCreated) updatedCount += 1;
        else unchangedCount += 1;
      }

      if (snapshotIdentityValid) {
        for (const task of existing) {
          const key = identityKey(task.source.repositoryNodeId, task.source.issueNodeId);
          if (observedIdentities.has(key)) continue;
          if (
            (task.sourceGate === "source-missing" || task.sourceGate === "identity-invalid") &&
            task.status === "needs-attention"
          ) {
            unchangedCount += 1;
            continue;
          }
          const commandId = yield* deriveAgentControlTaskCommandId([
            "agentControl.task.markNeedsAttention",
            task.taskId,
            String(task.revision),
            "source-missing",
            String(sourcePrecondition.githubIntakeSequence),
          ]);
          const result = yield* engine.dispatchController({
            type: "agentControl.task.markNeedsAttention",
            commandId,
            taskId: task.taskId,
            projectId,
            expectedRevision: task.revision,
            sourcePrecondition,
            source: task.source,
            sourceGate: "source-missing",
            sourceUpdatedAt: task.sourceUpdatedAt,
            githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
            sourceSnapshot: task.sourceSnapshot,
          });
          if (result.eventCreated) needsAttentionCount += 1;
          else unchangedCount += 1;
        }
      }

      // Give concurrently committed poll/config work a scheduling boundary
      // before the linear completion check. Correctness still comes solely
      // from the transactional precondition, not from this yield.
      yield* Effect.yieldNow;
      yield* engine.verifySourceSnapshot(sourcePrecondition);
      return {
        projectId,
        githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
        observedCount: issues.length,
        createdCount,
        updatedCount,
        needsAttentionCount,
        unchangedCount,
      } satisfies AgentControlTaskReconcileOnceResult;
    });

    const result = yield* Effect.result(execute);
    const finishedAt = DateTime.formatIso(yield* DateTime.now);
    if (result._tag === "Failure") {
      yield* reconciles
        .markRecoveryRequired(
          projectId,
          sourcePrecondition.githubIntakeSequence,
          watermark.revision,
          finishedAt,
        )
        .pipe(Effect.ignore);
      return yield* result.failure;
    }
    yield* reconciles
      .complete(projectId, sourcePrecondition.githubIntakeSequence, watermark.revision, finishedAt)
      .pipe(
        Effect.mapError(() => safeError("internal-persistence-error", "reconcile-once", projectId)),
      );
    return result.success;
  });

  const runWithRetry = Effect.fn("AgentControlTaskIntake.runWithRetry")(function* (
    projectId: ProjectId,
  ) {
    let lastRetryable: AgentControlTaskRpcError | null = null;
    for (let attempt = 1; attempt <= MAX_RECONCILE_ATTEMPTS; attempt += 1) {
      const result = yield* Effect.result(runPass(projectId));
      if (result._tag === "Success") return result.success;
      if (
        result.failure.code !== "revision-conflict" &&
        result.failure.code !== "source-snapshot-stale"
      ) {
        return yield* result.failure;
      }
      lastRetryable = result.failure;
      if (attempt < MAX_RECONCILE_ATTEMPTS) yield* Effect.yieldNow;
    }
    return yield* lastRetryable ?? safeError("source-snapshot-stale", "reconcile-once", projectId);
  });

  const reconcileOnce: AgentControlTaskIntakeShape["reconcileOnce"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeReconcile(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "reconcile-once", rawInput.projectId)),
      );
      const lock = yield* getProjectLock(input.projectId);
      return yield* lock.withPermit(runWithRetry(input.projectId));
    });

  return AgentControlTaskIntake.of({ getTask, listTasks, reconcileOnce });
});

export const layer = Layer.effect(AgentControlTaskIntake, makeIntake);
