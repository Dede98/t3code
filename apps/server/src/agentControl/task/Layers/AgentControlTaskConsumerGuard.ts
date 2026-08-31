import type {
  AgentControlGithubIssueSnapshot,
  AgentControlRunOnceId,
  AgentControlTaskId,
  AgentControlTaskState,
  AgentControlTaskSourceSnapshot,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { sameTaskSourceSnapshot } from "../decider.ts";
import { loadAuthoritativeTaskProjectHistory } from "../authoritative.ts";
import { canonicalAgentControlTaskSourceTimestamp } from "../sourceTimestamp.ts";
import { fingerprintAgentControlRunOnceSource } from "../../runOnce/source.ts";
import {
  AgentControlTaskConsumerGuard,
  AgentControlTaskConsumerGuardError,
  type AgentControlTaskConsumerGuardReason,
  type AgentControlTaskConsumerGuardShape,
  type AgentControlTaskProjectGate,
} from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";

const guardError = (projectId: ProjectId, reason: AgentControlTaskConsumerGuardReason) =>
  new AgentControlTaskConsumerGuardError({ projectId, reason });

const canonicalTaskSnapshot = (
  issue: AgentControlGithubIssueSnapshot,
): AgentControlTaskSourceSnapshot | null => {
  const updatedAt = canonicalAgentControlTaskSourceTimestamp(issue.updatedAt);
  if (updatedAt === null) return null;
  return {
    repositoryNodeId: issue.repositoryNodeId,
    issueNodeId: issue.issueNodeId,
    number: issue.number,
    url: issue.url,
    state: issue.state,
    title: issue.title,
    body: issue.body,
    contentTrust: "untrusted-external",
    updatedAt,
    timelineComplete: issue.timelineComplete,
    ready: issue.ready,
    paused: issue.paused,
    eligible: issue.eligible,
    eligibilityReason: issue.eligibilityReason,
  };
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const availability = yield* AgentControlProjectAvailability;
  const projects = yield* AgentControlProjectStateRepository;
  const github = yield* AgentControlGithubStateRepository;
  const reconciles = yield* AgentControlTaskReconcileStateRepository;
  const tasks = yield* AgentControlTaskStateRepository;
  const taskEvents = yield* AgentControlTaskEventStore;

  const readProjectGate = Effect.fn("AgentControlTaskConsumerGuard.readProjectGate")(function* (
    projectId: ProjectId,
  ) {
    const watermark = yield* reconciles
      .get(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    const watermarkFields = Option.match(watermark, {
      onNone: () => ({
        targetSequence: null,
        lastCompletedSequence: null,
        watermarkCompleted: false,
      }),
      onSome: (state) => ({
        targetSequence: state.targetSequence,
        lastCompletedSequence: state.lastCompletedSequence,
        watermarkCompleted: state.status === "completed",
      }),
    });

    const available = yield* Effect.result(availability.ensureAvailable(projectId));
    if (available._tag === "Failure") {
      if (available.failure._tag !== "AgentControlProjectUnavailableError") {
        return yield* guardError(projectId, "internal-persistence-error");
      }
      return {
        projectId,
        activation: "inactive",
        currentSourceSequence: null,
        ...watermarkFields,
        sequenceCurrent: false,
        sourceFingerprint: null,
        reason: "project-unavailable",
      } satisfies AgentControlTaskProjectGate;
    }

    const project = yield* projects
      .get(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    if (Option.isNone(project) || project.value.mode !== "observe") {
      return {
        projectId,
        activation: "inactive",
        currentSourceSequence: null,
        ...watermarkFields,
        sequenceCurrent: false,
        sourceFingerprint: null,
        reason: "mode-inactive",
      } satisfies AgentControlTaskProjectGate;
    }

    const source = yield* github
      .getCompletedSnapshot(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    if (Option.isNone(source)) {
      return {
        projectId,
        activation: "waiting-source",
        currentSourceSequence: null,
        ...watermarkFields,
        sequenceCurrent: false,
        sourceFingerprint: null,
        reason: "source-snapshot-unavailable",
      } satisfies AgentControlTaskProjectGate;
    }

    const sourceSequence = source.value.sourcePrecondition.githubIntakeSequence;
    const taskHistory = yield* loadAuthoritativeTaskProjectHistory(
      projectId,
      taskEvents,
      tasks,
    ).pipe(
      Effect.mapError((failure) =>
        guardError(
          projectId,
          failure._tag === "AgentControlPersistenceSqlError"
            ? "internal-persistence-error"
            : "task-projection-corrupt",
        ),
      ),
    );
    const taskProjectionCurrent = taskHistory.every(
      (task) => task.githubIntakeSequence === sourceSequence,
    );
    const sequenceCurrent =
      Option.isSome(watermark) &&
      watermark.value.status === "completed" &&
      watermark.value.targetSequence === watermark.value.lastCompletedSequence &&
      watermark.value.lastCompletedSequence === sourceSequence &&
      taskProjectionCurrent;

    return {
      projectId,
      activation: "observe",
      currentSourceSequence: sourceSequence,
      ...watermarkFields,
      sequenceCurrent,
      sourceFingerprint: fingerprintAgentControlRunOnceSource(source.value.sourcePrecondition),
      reason: null,
    } satisfies AgentControlTaskProjectGate;
  });

  const ensureProjectCurrent = Effect.fn("AgentControlTaskConsumerGuard.ensureProjectCurrent")(
    function* (projectId: ProjectId) {
      const gate = yield* readProjectGate(projectId);
      if (gate.activation === "inactive") {
        return yield* guardError(projectId, gate.reason ?? "mode-inactive");
      }
      if (gate.activation === "waiting-source") {
        return yield* guardError(projectId, "source-snapshot-unavailable");
      }
      if (gate.reason === "task-projection-corrupt") {
        return yield* guardError(projectId, "task-projection-corrupt");
      }
      if (gate.targetSequence === null || gate.lastCompletedSequence === null) {
        return yield* guardError(projectId, "watermark-missing");
      }
      if (gate.targetSequence !== gate.lastCompletedSequence) {
        return yield* guardError(projectId, "watermark-sequence-mismatch");
      }
      if (!gate.watermarkCompleted) {
        return yield* guardError(projectId, "watermark-not-completed");
      }
      return gate;
    },
  );

  const inspectProject: AgentControlTaskConsumerGuardShape["inspectProject"] = (projectId) =>
    sql
      .withTransaction(readProjectGate(projectId))
      .pipe(
        Effect.mapError((error) =>
          error._tag === "AgentControlTaskConsumerGuardError"
            ? error
            : guardError(projectId, "internal-persistence-error"),
        ),
      );

  const useValidatedTask = <A, E, R>(
    projectId: ProjectId,
    taskId: AgentControlTaskId,
    gate: AgentControlTaskProjectGate,
    use: (task: AgentControlTaskState, gate: AgentControlTaskProjectGate) => Effect.Effect<A, E, R>,
  ) =>
    Effect.gen(function* () {
      const taskHistory = yield* loadAuthoritativeTaskProjectHistory(
        projectId,
        taskEvents,
        tasks,
      ).pipe(
        Effect.mapError((failure) =>
          guardError(
            projectId,
            failure._tag === "AgentControlPersistenceSqlError"
              ? "internal-persistence-error"
              : "task-projection-corrupt",
          ),
        ),
      );
      const matchingTasks = taskHistory.filter((task) => task.taskId === taskId);
      if (matchingTasks.length === 0) {
        return yield* guardError(projectId, "task-missing");
      }
      if (matchingTasks.length !== 1) {
        return yield* guardError(projectId, "task-projection-corrupt");
      }
      const task = matchingTasks[0]!;
      if (task.source.projectId !== projectId) {
        return yield* guardError(projectId, "task-project-mismatch");
      }
      if (task.status !== "candidate") {
        return yield* guardError(projectId, "task-status-inactive");
      }
      if (task.sourceGate !== "eligible") {
        return yield* guardError(projectId, "task-source-ineligible");
      }
      if (task.stage !== "intake") {
        return yield* guardError(projectId, "task-stage-inactive");
      }
      if (task.githubIntakeSequence !== gate.currentSourceSequence) {
        return yield* guardError(projectId, "task-sequence-mismatch");
      }
      if (!gate.sequenceCurrent) {
        return yield* guardError(projectId, "watermark-not-completed");
      }

      const source = yield* github
        .getCompletedSnapshot(projectId)
        .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
      if (Option.isNone(source)) {
        return yield* guardError(projectId, "source-snapshot-unavailable");
      }
      const issue = source.value.issues.find(
        (candidate) =>
          candidate.repositoryNodeId === task.source.repositoryNodeId &&
          candidate.issueNodeId === task.source.issueNodeId,
      );
      if (
        issue === undefined ||
        issue.number !== task.source.issueNumber ||
        issue.url !== task.source.issueUrl ||
        issue.state !== "open" ||
        !issue.timelineComplete ||
        !issue.ready ||
        issue.paused ||
        !issue.eligible ||
        issue.eligibilityReason !== "eligible"
      ) {
        return yield* guardError(projectId, "task-source-mismatch");
      }
      const snapshot = canonicalTaskSnapshot(issue);
      if (
        snapshot === null ||
        task.sourceUpdatedAt !== snapshot.updatedAt ||
        !sameTaskSourceSnapshot(task.sourceSnapshot, snapshot)
      ) {
        return yield* guardError(projectId, "task-source-mismatch");
      }
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const callbackFiber = yield* use(task, gate).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          return yield* restore(Fiber.join(callbackFiber)).pipe(
            Effect.onExit(() => Fiber.interrupt(callbackFiber).pipe(Effect.asVoid)),
          );
        }),
      );
    });

  const loadRunOnceGate = Effect.fn("AgentControlTaskConsumerGuard.loadRunOnceGate")(function* (
    requestedRunId: AgentControlRunOnceId,
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) {
    const rows = yield* sql<{
      readonly runId: unknown;
      readonly githubIntakeSequence: unknown;
      readonly sourceFingerprint: unknown;
      readonly reconcileRevision: unknown;
      readonly projectMode: unknown;
      readonly pausedFromMode: unknown;
    }>`
      SELECT activation.run_id AS "runId",
             activation.github_intake_sequence AS "githubIntakeSequence",
             activation.source_fingerprint AS "sourceFingerprint",
             activation.reconcile_revision AS "reconcileRevision",
             project.mode AS "projectMode", project.paused_from_mode AS "pausedFromMode"
      FROM main.agent_control_run_once_activations activation
      JOIN main.agent_control_run_once_states state ON state.run_id = activation.run_id
      JOIN main.agent_control_project_states project ON project.project_id = activation.project_id
      JOIN main.agent_control_run_once_step_evidence selected
        ON selected.run_id = activation.run_id AND selected.step = 'task-selected'
      JOIN main.agent_control_run_once_step_receipts receipt
        ON receipt.evidence_id = selected.evidence_id AND receipt.status = 'accepted'
      JOIN main.agent_control_run_once_step_markers marker
        ON marker.evidence_id = selected.evidence_id AND marker.receipt_id = receipt.receipt_id
      WHERE activation.project_id = ${projectId}
        AND state.status = 'active'
        AND selected.task_id = ${taskId}
        AND activation.run_id = ${requestedRunId}
        AND project.mode = 'run-once'
        AND project.paused_from_mode IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM main.agent_control_run_once_step_markers terminal
          WHERE terminal.run_id = activation.run_id AND terminal.step = 'completed'
        )
    `.pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    if (rows.length !== 1) {
      return yield* guardError(
        projectId,
        rows.length === 0 ? "mode-inactive" : "task-projection-corrupt",
      );
    }
    const row = rows[0]!;
    if (
      typeof row.runId !== "string" ||
      row.runId !== requestedRunId ||
      typeof row.githubIntakeSequence !== "number" ||
      !Number.isSafeInteger(row.githubIntakeSequence) ||
      row.githubIntakeSequence < 1 ||
      typeof row.sourceFingerprint !== "string" ||
      !/^[0-9a-f]{64}$/u.test(row.sourceFingerprint) ||
      typeof row.reconcileRevision !== "number" ||
      !Number.isSafeInteger(row.reconcileRevision) ||
      row.reconcileRevision < 1 ||
      row.projectMode !== "run-once" ||
      row.pausedFromMode !== null
    ) {
      return yield* guardError(projectId, "task-projection-corrupt");
    }
    const reconcile = yield* reconciles
      .get(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    if (
      Option.isNone(reconcile) ||
      reconcile.value.status !== "completed" ||
      reconcile.value.targetSequence !== row.githubIntakeSequence ||
      reconcile.value.lastCompletedSequence !== row.githubIntakeSequence ||
      reconcile.value.revision !== row.reconcileRevision
    ) {
      return yield* guardError(projectId, "watermark-not-completed");
    }
    const source = yield* github
      .getCompletedSnapshot(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    if (
      Option.isNone(source) ||
      source.value.sourcePrecondition.githubIntakeSequence !== row.githubIntakeSequence ||
      fingerprintAgentControlRunOnceSource(source.value.sourcePrecondition) !==
        row.sourceFingerprint
    ) {
      return yield* guardError(projectId, "task-source-mismatch");
    }
    return {
      projectId,
      activation: "observe",
      currentSourceSequence: row.githubIntakeSequence,
      targetSequence: reconcile.value.targetSequence,
      lastCompletedSequence: reconcile.value.lastCompletedSequence,
      watermarkCompleted: true,
      sequenceCurrent: true,
      sourceFingerprint: row.sourceFingerprint,
      reason: null,
    } satisfies AgentControlTaskProjectGate;
  });

  const useTaskSelectedForRunOnceInTransaction: NonNullable<
    AgentControlTaskConsumerGuardShape["useTaskSelectedForRunOnceInTransaction"]
  > = (runId, projectId, taskId, use) =>
    Effect.gen(function* () {
      const gate = yield* loadRunOnceGate(runId, projectId, taskId);
      return yield* useValidatedTask(projectId, taskId, gate, use);
    });

  const useTaskConsumableInTransaction: AgentControlTaskConsumerGuardShape["useTaskConsumableInTransaction"] =
    (projectId, taskId, use) =>
      Effect.gen(function* () {
        const observe = yield* ensureProjectCurrent(projectId);
        return yield* useValidatedTask(projectId, taskId, observe, use);
      });

  const useTaskConsumable: AgentControlTaskConsumerGuardShape["useTaskConsumable"] = (
    projectId,
    taskId,
    use,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const gate = yield* ensureProjectCurrent(projectId);
          return yield* useValidatedTask(projectId, taskId, gate, use);
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", () =>
          Effect.fail(guardError(projectId, "internal-persistence-error")),
        ),
      );

  const useTaskSelectedForRunOnce: NonNullable<
    AgentControlTaskConsumerGuardShape["useTaskSelectedForRunOnce"]
  > = (runId, projectId, taskId, use) =>
    sql
      .withTransaction(useTaskSelectedForRunOnceInTransaction(runId, projectId, taskId, use))
      .pipe(
        Effect.catchTag("SqlError", () =>
          Effect.fail(guardError(projectId, "internal-persistence-error")),
        ),
      );

  return AgentControlTaskConsumerGuard.of({
    inspectProject,
    useTaskConsumable,
    useTaskConsumableInTransaction,
    useTaskSelectedForRunOnce,
    useTaskSelectedForRunOnceInTransaction,
  });
});

export const layer = Layer.effect(AgentControlTaskConsumerGuard, make);
