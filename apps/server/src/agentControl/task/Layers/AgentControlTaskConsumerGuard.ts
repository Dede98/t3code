import type {
  AgentControlGithubIssueSnapshot,
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
import { canonicalAgentControlTaskSourceTimestamp } from "../sourceTimestamp.ts";
import {
  AgentControlTaskConsumerGuard,
  AgentControlTaskConsumerGuardError,
  type AgentControlTaskConsumerGuardReason,
  type AgentControlTaskConsumerGuardShape,
  type AgentControlTaskProjectGate,
} from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";

const guardError = (projectId: ProjectId, reason: AgentControlTaskConsumerGuardReason) =>
  new AgentControlTaskConsumerGuardError({ projectId, reason });

const fingerprint = (input: {
  readonly githubIntakeSequence: number;
  readonly githubProjectionRevision: number;
  readonly githubConfigRevision: number;
  readonly repositoryNodeId: string;
}) =>
  [
    input.githubIntakeSequence,
    input.githubProjectionRevision,
    input.githubConfigRevision,
    input.repositoryNodeId,
  ].join(":");

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
    const taskEntries = yield* tasks
      .listProject(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    const taskProjectionCurrent = taskEntries.every(
      (entry) => entry._tag === "Valid" && entry.state.githubIntakeSequence === sourceSequence,
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
      sourceFingerprint: fingerprint(source.value.sourcePrecondition),
      reason: taskEntries.some((entry) => entry._tag === "Corrupt")
        ? "task-projection-corrupt"
        : null,
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

  const useTaskConsumable: AgentControlTaskConsumerGuardShape["useTaskConsumable"] = (
    projectId,
    taskId,
    use,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const gate = yield* ensureProjectCurrent(projectId);
          const taskResult = yield* Effect.result(tasks.get(taskId));
          if (taskResult._tag === "Failure") {
            return yield* guardError(projectId, "task-projection-corrupt");
          }
          if (Option.isNone(taskResult.success)) {
            return yield* guardError(projectId, "task-missing");
          }
          const task = taskResult.success.value;
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
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", () =>
          Effect.fail(guardError(projectId, "internal-persistence-error")),
        ),
      );

  return AgentControlTaskConsumerGuard.of({ inspectProject, useTaskConsumable });
});

export const layer = Layer.effect(AgentControlTaskConsumerGuard, make);
