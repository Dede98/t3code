import type { AgentControlTaskState, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
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

const make = Effect.gen(function* () {
  const availability = yield* AgentControlProjectAvailability;
  const projects = yield* AgentControlProjectStateRepository;
  const github = yield* AgentControlGithubStateRepository;
  const reconciles = yield* AgentControlTaskReconcileStateRepository;
  const tasks = yield* AgentControlTaskStateRepository;

  const inspectProject: AgentControlTaskConsumerGuardShape["inspectProject"] = Effect.fn(
    "AgentControlTaskConsumerGuard.inspectProject",
  )(function* (projectId) {
    const watermark = yield* reconciles
      .get(projectId)
      .pipe(Effect.mapError(() => guardError(projectId, "internal-persistence-error")));
    const watermarkFields = Option.match(watermark, {
      onNone: () => ({ targetSequence: null, lastCompletedSequence: null }),
      onSome: (state) => ({
        targetSequence: state.targetSequence,
        lastCompletedSequence: state.lastCompletedSequence,
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

  const ensureCurrent: AgentControlTaskConsumerGuardShape["ensureCurrent"] = Effect.fn(
    "AgentControlTaskConsumerGuard.ensureCurrent",
  )(function* (projectId: ProjectId, task?: AgentControlTaskState) {
    const gate = yield* inspectProject(projectId);
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
    if (!gate.sequenceCurrent) {
      return yield* guardError(projectId, "watermark-not-completed");
    }
    if (
      task !== undefined &&
      (task.source.projectId !== projectId ||
        task.githubIntakeSequence !== gate.currentSourceSequence)
    ) {
      return yield* guardError(projectId, "task-sequence-mismatch");
    }
    return gate;
  });

  return AgentControlTaskConsumerGuard.of({ inspectProject, ensureCurrent });
});

export const layer = Layer.effect(AgentControlTaskConsumerGuard, make);
