import {
  AgentControlTaskGetInput,
  AgentControlTaskListInput,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  type AgentControlTaskReconcileOnceResult,
  type AgentControlTaskState,
  type AgentControlTaskSummary,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { deriveAgentControlTaskCommandId, deriveAgentControlTaskId } from "../identity.ts";
import { buildReconcilePlan } from "../reconcilePlan.ts";
import { makeAgentControlTaskReconcileLocks } from "../reconcileLocks.ts";
import {
  AgentControlTaskIntake,
  type AgentControlTaskIntakeShape,
} from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

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

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const availability = yield* AgentControlProjectAvailability;
  const projects = yield* AgentControlProjectStateRepository;
  const github = yield* AgentControlGithubStateRepository;
  const engine = yield* AgentControlTaskEngine;
  const states = yield* AgentControlTaskStateRepository;
  const reconciles = yield* AgentControlTaskReconcileStateRepository;
  const locks = yield* makeAgentControlTaskReconcileLocks<AgentControlTaskGetInput["projectId"]>();

  const useCanonicalSource = <A, E, R>(
    precondition: Parameters<AgentControlTaskEngine["Service"]["verifySourceSnapshot"]>[0],
    observeOnly: boolean,
    use: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, AgentControlTaskRpcError | E, R> =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* ensureProject(availability, precondition.projectId, "reconcile-once");
          if (observeOnly) {
            const project = yield* projects
              .get(precondition.projectId)
              .pipe(
                Effect.mapError(() =>
                  safeError("internal-persistence-error", "reconcile-once", precondition.projectId),
                ),
              );
            if (
              Option.isNone(project) ||
              (project.value.mode !== "observe" && project.value.mode !== "armed")
            ) {
              return yield* safeError(
                "project-mode-inactive",
                "reconcile-once",
                precondition.projectId,
              );
            }
          }
          const matches = yield* github
            .matchesCompletedSnapshot(precondition)
            .pipe(
              Effect.mapError(() =>
                safeError("internal-persistence-error", "reconcile-once", precondition.projectId),
              ),
            );
          if (!matches) {
            return yield* safeError(
              "source-snapshot-stale",
              "reconcile-once",
              precondition.projectId,
            );
          }
          return yield* use;
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", () =>
          Effect.fail(
            safeError("internal-persistence-error", "reconcile-once", precondition.projectId),
          ),
        ),
      );

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
    return {
      sourcePrecondition: source.value.sourcePrecondition,
      issues: source.value.issues,
      existing,
    };
  });

  const runPass = Effect.fn("AgentControlTaskIntake.runPass")(function* (
    projectId: ProjectId,
    observeOnly: boolean,
  ) {
    const pass = yield* loadPassSnapshot(projectId);
    const { sourcePrecondition, issues, existing } = pass;
    const plan = buildReconcilePlan({ sourcePrecondition, issues }, existing);
    if (!plan.classifiable) {
      const currentWatermark = yield* reconciles
        .get(projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "reconcile-once", projectId),
          ),
        );
      if (
        Option.isSome(currentWatermark) &&
        currentWatermark.value.status === "reconciling" &&
        currentWatermark.value.targetSequence === sourcePrecondition.githubIntakeSequence
      ) {
        const recoveryAt = DateTime.formatIso(yield* DateTime.now);
        yield* reconciles
          .markRecoveryRequired(
            projectId,
            currentWatermark.value.targetSequence,
            currentWatermark.value.revision,
            recoveryAt,
          )
          .pipe(Effect.ignore);
      }
      return yield* safeError("source-identity-conflict", "reconcile-once", projectId);
    }
    const startedAt = DateTime.formatIso(yield* DateTime.now);
    const watermark = yield* useCanonicalSource(
      sourcePrecondition,
      observeOnly,
      reconciles
        .begin(projectId, sourcePrecondition.githubIntakeSequence, startedAt)
        .pipe(
          Effect.mapError((failure) =>
            failure._tag === "AgentControlTaskReconcileConflictError"
              ? safeError("source-snapshot-stale", "reconcile-once", projectId)
              : safeError("internal-persistence-error", "reconcile-once", projectId),
          ),
        ),
    );
    const dispatch = observeOnly ? engine.dispatchObservedController : engine.dispatchController;

    const execute = Effect.gen(function* () {
      let createdCount = 0;
      let updatedCount = 0;
      let needsAttentionCount = 0;
      let unchangedCount = 0;
      for (const operation of plan.operations) {
        switch (operation.type) {
          case "unchanged":
            unchangedCount += 1;
            break;
          case "create": {
            const { issue, gate, snapshot, sourceUpdatedAt } = operation;
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
              sourceUpdatedAt,
              gate,
              String(sourcePrecondition.githubIntakeSequence),
            ]);
            const result = yield* dispatch({
              type: "agentControl.task.createFromGithubIssue",
              commandId,
              taskId,
              projectId,
              expectedRevision: 0,
              sourcePrecondition,
              source,
              sourceGate: gate,
              sourceUpdatedAt,
              githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
              sourceSnapshot: snapshot,
            });
            if (result.eventCreated) createdCount += 1;
            else unchangedCount += 1;
            break;
          }
          case "recover-source-missing": {
            const { task, snapshot, sourceUpdatedAt } = operation;
            const commandId = yield* deriveAgentControlTaskCommandId([
              "agentControl.task.recoverSourceMissing",
              task.taskId,
              String(task.revision),
              sourceUpdatedAt,
              String(sourcePrecondition.githubIntakeSequence),
            ]);
            const result = yield* dispatch({
              type: "agentControl.task.recoverSourceMissing",
              commandId,
              taskId: task.taskId,
              projectId,
              expectedRevision: task.revision,
              sourcePrecondition,
              source: task.source,
              sourceGate: "eligible",
              sourceUpdatedAt,
              githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
              sourceSnapshot: snapshot,
            });
            if (result.eventCreated) updatedCount += 1;
            else unchangedCount += 1;
            break;
          }
          case "refresh": {
            const { task, gate, snapshot, sourceUpdatedAt } = operation;
            const commandId = yield* deriveAgentControlTaskCommandId([
              "agentControl.task.sourceGate.refresh",
              task.taskId,
              String(task.revision),
              sourceUpdatedAt,
              gate,
              String(sourcePrecondition.githubIntakeSequence),
            ]);
            const result = yield* dispatch({
              type: "agentControl.task.sourceGate.refresh",
              commandId,
              taskId: task.taskId,
              projectId,
              expectedRevision: task.revision,
              sourcePrecondition,
              source: task.source,
              sourceGate: gate,
              sourceUpdatedAt,
              githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
              sourceSnapshot: snapshot,
            });
            if (result.eventCreated) updatedCount += 1;
            else unchangedCount += 1;
            break;
          }
          case "mark-identity-invalid":
          case "mark-source-missing": {
            const task = operation.task;
            const gate =
              operation.type === "mark-identity-invalid"
                ? ("identity-invalid" as const)
                : ("source-missing" as const);
            const commandId = yield* deriveAgentControlTaskCommandId([
              "agentControl.task.markNeedsAttention",
              task.taskId,
              String(task.revision),
              gate,
              String(sourcePrecondition.githubIntakeSequence),
            ]);
            const result = yield* dispatch({
              type: "agentControl.task.markNeedsAttention",
              commandId,
              taskId: task.taskId,
              projectId,
              expectedRevision: task.revision,
              sourcePrecondition,
              source: task.source,
              sourceGate: gate,
              sourceUpdatedAt: task.sourceUpdatedAt,
              githubIntakeSequence: sourcePrecondition.githubIntakeSequence,
              sourceSnapshot: task.sourceSnapshot,
            });
            if (result.eventCreated) needsAttentionCount += 1;
            else unchangedCount += 1;
            break;
          }
        }
      }

      // Give concurrently committed poll/config work a scheduling boundary
      // before the linear completion check. Correctness still comes solely
      // from the transactional precondition, not from this yield.
      yield* Effect.yieldNow;
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
    const completed = yield* Effect.result(
      useCanonicalSource(
        sourcePrecondition,
        observeOnly,
        reconciles
          .complete(
            projectId,
            sourcePrecondition.githubIntakeSequence,
            watermark.revision,
            finishedAt,
          )
          .pipe(
            Effect.mapError((failure) =>
              failure._tag === "AgentControlTaskReconcileConflictError"
                ? safeError("source-snapshot-stale", "reconcile-once", projectId)
                : safeError("internal-persistence-error", "reconcile-once", projectId),
            ),
          ),
      ),
    );
    if (completed._tag === "Failure") {
      yield* reconciles
        .markRecoveryRequired(
          projectId,
          sourcePrecondition.githubIntakeSequence,
          watermark.revision,
          finishedAt,
        )
        .pipe(Effect.ignore);
      return yield* completed.failure;
    }
    return result.success;
  });

  const runWithRetry = Effect.fn("AgentControlTaskIntake.runWithRetry")(function* (
    projectId: ProjectId,
    observeOnly: boolean,
  ) {
    let lastRetryable: AgentControlTaskRpcError | null = null;
    for (let attempt = 1; attempt <= MAX_RECONCILE_ATTEMPTS; attempt += 1) {
      const result = yield* Effect.result(runPass(projectId, observeOnly));
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
      return yield* locks.withLock(
        input.projectId,
        ensureProject(availability, input.projectId, "reconcile-once"),
        runWithRetry(input.projectId, false),
      );
    });

  const reconcileObservedProject: AgentControlTaskIntakeShape["reconcileObservedProject"] = (
    rawInput,
  ) =>
    Effect.gen(function* () {
      const input = yield* decodeReconcile(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "reconcile-once", rawInput.projectId)),
      );
      return yield* locks.withLock(
        input.projectId,
        ensureProject(availability, input.projectId, "reconcile-once"),
        runWithRetry(input.projectId, true),
      );
    });

  return AgentControlTaskIntake.of({
    getTask,
    listTasks,
    reconcileOnce,
    reconcileObservedProject,
  });
});

export const layer = Layer.effect(AgentControlTaskIntake, make);
