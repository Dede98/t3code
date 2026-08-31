import {
  AgentControlRunOnceId,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  type AgentControlRunOnceActivation,
  type AgentControlRunOnceStep,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlEventStore } from "../../../persistence/Services/AgentControlEventStore.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { createDefaultAgentControlProjectState } from "../../decider.ts";
import { projectAgentControlEvent } from "../../projector.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlControlledThreadActivation } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import {
  createDefaultGithubIntakeState,
  projectGithubIntakeEvent,
} from "../../github/projector.ts";
import { AgentControlGithubEventStore } from "../../github/Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { loadAuthoritativeTaskProjectHistory } from "../../task/authoritative.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskEventStore } from "../../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskReconcileStateRepository } from "../../task/Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { canonicalJson, type JsonValue } from "../../initialPlanning/eventEvidence.ts";
import {
  admitRunOnceActivation,
  loadRunOnceTerminalAuthority,
  writeRunOnceStep,
  writeRunOnceStepInTransaction,
  type RunOnceStateBinding,
} from "../authority.ts";
import { deriveAgentControlRunOnceId, deriveRunOnceCommandId } from "../identity.ts";
import { AgentControlRunOnceError, type RunOnceStepBindings } from "../model.ts";
import { selectAgentControlRunOnceCandidate } from "../selection.ts";
import { fingerprintAgentControlRunOnceSource } from "../source.ts";
import {
  AgentControlRunOnceController,
  type AgentControlRunOnceCommittedPublication,
  type AgentControlRunOnceControllerShape,
} from "../Services/AgentControlRunOnceController.ts";
import { AgentControlRunOnceControllerHooks } from "../Services/AgentControlRunOnceControllerHooks.ts";
import { requireRunOnceMethod } from "../context.ts";

const PAGE_SIZE = 500;
const LEASE_DURATION_MS = 60_000;

interface PersistedRunState extends RunOnceStateBinding {
  readonly runId: AgentControlRunOnceId;
  readonly nextOrdinal: number;
  readonly lastStep: AgentControlRunOnceStep;
}

const error = (
  projectId: ProjectId,
  runId: AgentControlRunOnceId | null,
  step: AgentControlRunOnceStep | null,
  reason: AgentControlRunOnceError["reason"],
  cause?: unknown,
) =>
  new AgentControlRunOnceError({
    projectId,
    runId,
    step,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const same = (left: unknown, right: unknown) =>
  canonicalJson(left as JsonValue) === canonicalJson(right as JsonValue);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projectEvents = yield* AgentControlEventStore;
  const projectStates = yield* AgentControlProjectStateRepository;
  const githubEvents = yield* AgentControlGithubEventStore;
  const githubStates = yield* AgentControlGithubStateRepository;
  const reconciles = yield* AgentControlTaskReconcileStateRepository;
  const taskEvents = yield* AgentControlTaskEventStore;
  const taskStates = yield* AgentControlTaskStateRepository;
  const taskEngine = yield* AgentControlTaskEngine;
  const projectEngine = yield* AgentControlEngine;
  const stageRuns = yield* AgentControlStageRun;
  const leases = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktreeController;
  const threads = yield* AgentControlControlledThreadActivation;
  const hooks = yield* AgentControlRunOnceControllerHooks;
  const publications = yield* PubSub.unbounded<AgentControlRunOnceCommittedPublication>();
  const projectLocks = yield* SynchronizedRef.make(new Map<ProjectId, Semaphore.Semaphore>());
  const getProjectLock = (projectId: ProjectId) =>
    SynchronizedRef.modifyEffect(projectLocks, (current) => {
      const existing = current.get(projectId);
      if (existing !== undefined) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => {
          const next = new Map(current);
          next.set(projectId, lock);
          return [lock, next] as const;
        }),
      );
    });

  const readProjectHistory = Effect.fn("AgentControlRunOnce.readProjectHistory")(function* (
    projectId: ProjectId,
  ) {
    const all: Array<AgentControlEvent> = [];
    let cursor = 0;
    while (true) {
      const page = yield* projectEvents
        .readStream(projectId, cursor, PAGE_SIZE)
        .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
      if (page.length === 0) break;
      for (const event of page) {
        if (event.streamVersion !== cursor + 1) {
          return yield* error(projectId, null, null, "authority-conflict");
        }
        all.push(event);
        cursor = event.streamVersion;
      }
    }
    let folded = createDefaultAgentControlProjectState(projectId);
    for (const event of all) {
      folded = yield* projectAgentControlEvent(folded, event).pipe(
        Effect.mapError((cause) => error(projectId, null, null, "authority-conflict", cause)),
      );
    }
    const projected = yield* projectStates
      .get(projectId)
      .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
    const current = Option.getOrElse(projected, () =>
      createDefaultAgentControlProjectState(projectId),
    );
    if (!same(folded, current)) return yield* error(projectId, null, null, "projection-corrupt");
    return { events: all, state: current } as const;
  });

  const readGithubHistory = Effect.fn("AgentControlRunOnce.readGithubHistory")(function* (
    projectId: ProjectId,
  ) {
    const all: Array<AgentControlGithubEvent> = [];
    let cursor = 0;
    while (true) {
      const page = yield* githubEvents
        .readStream(projectId, cursor, PAGE_SIZE)
        .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
      if (page.length === 0) break;
      for (const event of page) {
        if (event.streamVersion !== cursor + 1) {
          return yield* error(projectId, null, null, "source-unavailable");
        }
        all.push(event);
        cursor = event.streamVersion;
      }
    }
    let folded = createDefaultGithubIntakeState(projectId);
    for (const event of all) {
      folded = yield* projectGithubIntakeEvent(folded, event).pipe(
        Effect.mapError((cause) => error(projectId, null, null, "source-unavailable", cause)),
      );
    }
    const projected = yield* githubStates
      .get(projectId)
      .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
    if (Option.isNone(projected) || !same(folded, projected.value)) {
      return yield* error(projectId, null, null, "projection-corrupt");
    }
    return { events: all, state: folded } as const;
  });

  const loadRunState = Effect.fn("AgentControlRunOnce.loadRunState")(function* (
    projectId: ProjectId,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT run_id AS "runId", project_id AS "projectId", status,
        next_ordinal AS "nextOrdinal", last_step AS "lastStep", task_id AS "taskId",
        stage_run_id AS "stageRunId", lease_id AS "leaseId",
        worktree_reservation_id AS "worktreeReservationId",
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        terminal_task_event_id AS "terminalTaskEventId",
        activation_project_revision AS "activationProjectRevision",
        reset_project_revision AS "resetProjectRevision"
      FROM main.agent_control_run_once_states
      WHERE project_id = ${projectId} AND status = 'active'
    `.pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
    if (rows.length === 0) return null;
    const row = rows[0]!;
    if (
      rows.length !== 1 ||
      typeof row.runId !== "string" ||
      typeof row.nextOrdinal !== "number" ||
      !Number.isSafeInteger(row.nextOrdinal) ||
      typeof row.lastStep !== "string" ||
      row.projectId !== projectId ||
      row.status !== "active" ||
      typeof row.activationProjectRevision !== "number"
    )
      return yield* error(projectId, null, null, "projection-corrupt");
    return {
      runId: AgentControlRunOnceId.make(row.runId),
      projectId,
      status: "active",
      nextOrdinal: row.nextOrdinal,
      lastStep: row.lastStep as AgentControlRunOnceStep,
      taskId: typeof row.taskId === "string" ? row.taskId : null,
      stageRunId: typeof row.stageRunId === "string" ? row.stageRunId : null,
      leaseId: typeof row.leaseId === "string" ? row.leaseId : null,
      worktreeReservationId:
        typeof row.worktreeReservationId === "string" ? row.worktreeReservationId : null,
      controlledThreadReservationId:
        typeof row.controlledThreadReservationId === "string"
          ? row.controlledThreadReservationId
          : null,
      terminalTaskEventId:
        typeof row.terminalTaskEventId === "string" ? row.terminalTaskEventId : null,
      activationProjectRevision: row.activationProjectRevision,
      resetProjectRevision:
        typeof row.resetProjectRevision === "number" ? row.resetProjectRevision : null,
    } satisfies PersistedRunState;
  });

  const loadActivation = Effect.fn("AgentControlRunOnce.loadActivation")(function* (
    projectId: ProjectId,
    runId: AgentControlRunOnceId,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT run_id AS "runId", project_id AS "projectId",
        activation_event_id AS "activationEventId",
        activation_event_sequence AS "activationEventSequence",
        activation_event_stream_version AS "activationEventStreamVersion",
        activation_command_id AS "activationCommandId",
        github_intake_sequence AS "githubIntakeSequence", github_event_id AS "githubEventId",
        github_event_sequence AS "githubEventSequence",
        github_event_stream_version AS "githubEventStreamVersion",
        reconcile_revision AS "reconcileRevision", source_fingerprint AS "sourceFingerprint",
        activated_at AS "activatedAt"
      FROM main.agent_control_run_once_activations WHERE run_id = ${runId}
    `.pipe(Effect.mapError((cause) => error(projectId, runId, null, "persistence", cause)));
    if (rows.length !== 1) return yield* error(projectId, runId, null, "authority-conflict");
    return { schemaVersion: 1, ...rows[0]! } as unknown as AgentControlRunOnceActivation;
  });

  const newActivationAuthority = Effect.fn("AgentControlRunOnce.newActivationAuthority")(function* (
    projectId: ProjectId,
    existingActivation?: AgentControlRunOnceActivation,
  ) {
    const project = yield* readProjectHistory(projectId);
    const activationEvent =
      existingActivation === undefined
        ? project.events.at(-1)
        : project.events.find(
            (event) =>
              event.eventId === existingActivation.activationEventId &&
              event.sequence === existingActivation.activationEventSequence &&
              event.streamVersion === existingActivation.activationEventStreamVersion &&
              event.commandId === existingActivation.activationCommandId,
          );
    if (
      activationEvent === undefined ||
      activationEvent.authority !== "human" ||
      activationEvent.payload.previousMode !== "observe" ||
      activationEvent.payload.mode !== "run-once" ||
      activationEvent.payload.pausedFromMode !== null
    )
      return yield* error(projectId, null, "activation-admitted", "authority-conflict");

    if (existingActivation === undefined) {
      if (
        project.state.mode !== "run-once" ||
        project.state.pausedFromMode !== null ||
        activationEvent.streamVersion !== project.state.revision ||
        activationEvent.sequence !== project.state.sequence
      )
        return yield* error(projectId, null, "activation-admitted", "authority-conflict");
    } else {
      let expectedMode: "run-once" | "paused" = "run-once";
      for (const event of project.events.filter(
        (candidate) => candidate.streamVersion > activationEvent.streamVersion,
      )) {
        const isPause: boolean =
          expectedMode === "run-once" &&
          event.authority === "human" &&
          event.type === "agentControl.project.mode.changed" &&
          event.payload.previousMode === "run-once" &&
          event.payload.mode === "paused" &&
          event.payload.previousPausedFromMode === null &&
          event.payload.pausedFromMode === "run-once";
        const isResume: boolean =
          expectedMode === "paused" &&
          event.authority === "human" &&
          event.type === "agentControl.project.mode.changed" &&
          event.payload.previousMode === "paused" &&
          event.payload.mode === "run-once" &&
          event.payload.previousPausedFromMode === "run-once" &&
          event.payload.pausedFromMode === null;
        if (!isPause && !isResume) {
          return yield* error(
            projectId,
            existingActivation.runId,
            "task-selected",
            "authority-conflict",
          );
        }
        expectedMode = isPause ? "paused" : "run-once";
      }
      if (
        expectedMode !== "run-once" ||
        project.state.mode !== "run-once" ||
        project.state.pausedFromMode !== null
      )
        return yield* error(
          projectId,
          existingActivation.runId,
          "task-selected",
          "authority-conflict",
        );
    }

    const github = yield* readGithubHistory(projectId);
    const beforeActivation = github.events.filter(
      (event) => event.sequence < activationEvent.sequence,
    );
    const sourceEvent = beforeActivation
      .toReversed()
      .find((event) => event.type === "agentControl.github.poll.succeeded");
    if (
      sourceEvent === undefined ||
      beforeActivation.at(-1) !== sourceEvent ||
      sourceEvent.type !== "agentControl.github.poll.succeeded" ||
      github.state.sequence !== sourceEvent.sequence ||
      github.state.revision !== sourceEvent.streamVersion ||
      github.state.pollStatus.status !== "success" ||
      github.state.config === null
    )
      return yield* error(projectId, null, "activation-admitted", "source-unavailable");
    const snapshot = yield* githubStates
      .getCompletedSnapshot(projectId)
      .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
    if (Option.isNone(snapshot) || !same(snapshot.value.issues, sourceEvent.payload.issues)) {
      return yield* error(projectId, null, "activation-admitted", "source-unavailable");
    }
    const reconcile = yield* reconciles
      .get(projectId)
      .pipe(Effect.mapError((cause) => error(projectId, null, null, "persistence", cause)));
    if (
      Option.isNone(reconcile) ||
      reconcile.value.status !== "completed" ||
      reconcile.value.targetSequence !== sourceEvent.sequence ||
      reconcile.value.lastCompletedSequence !== sourceEvent.sequence
    )
      return yield* error(projectId, null, "activation-admitted", "source-watermark-stale");
    const tasks = yield* loadAuthoritativeTaskProjectHistory(
      projectId,
      taskEvents,
      taskStates,
    ).pipe(Effect.mapError((cause) => error(projectId, null, null, "task-history-corrupt", cause)));
    if (tasks.some((task) => task.githubIntakeSequence !== sourceEvent.sequence)) {
      return yield* error(projectId, null, "activation-admitted", "task-history-corrupt");
    }
    const runId = deriveAgentControlRunOnceId({
      projectId,
      activationEventId: activationEvent.eventId,
      activationEventSequence: activationEvent.sequence,
      activationEventStreamVersion: activationEvent.streamVersion,
      activationCommandId: activationEvent.commandId,
    });
    return {
      activation: {
        schemaVersion: 1,
        runId,
        projectId,
        activationEventId: activationEvent.eventId,
        activationEventSequence: activationEvent.sequence,
        activationEventStreamVersion: activationEvent.streamVersion,
        activationCommandId: activationEvent.commandId,
        githubIntakeSequence: sourceEvent.sequence,
        githubEventId: sourceEvent.eventId,
        githubEventSequence: sourceEvent.sequence,
        githubEventStreamVersion: sourceEvent.streamVersion,
        reconcileRevision: reconcile.value.revision,
        sourceFingerprint: fingerprintAgentControlRunOnceSource(snapshot.value.sourcePrecondition),
        activatedAt: activationEvent.occurredAt,
      } satisfies AgentControlRunOnceActivation,
      tasks,
    } as const;
  });

  const publishPending = Effect.fn("AgentControlRunOnce.publishPending")(function* (
    projectId?: ProjectId,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT publication.publication_id AS "publicationId", publication.run_id AS "runId",
        publication.ordinal, publication.step, activation.project_id AS "projectId"
      FROM main.agent_control_run_once_publications publication
      JOIN main.agent_control_run_once_step_markers marker
        ON marker.marker_id = publication.marker_id
      JOIN main.agent_control_run_once_activations activation
        ON activation.run_id = publication.run_id
      WHERE publication.published_at IS NULL
        AND (${projectId ?? null} IS NULL OR activation.project_id = ${projectId ?? null})
      ORDER BY publication.run_id, publication.ordinal
    `.pipe(
      Effect.mapError((cause) =>
        error(projectId ?? ("run-once-recovery" as ProjectId), null, null, "persistence", cause),
      ),
    );
    for (const row of rows) {
      if (
        typeof row.publicationId !== "string" ||
        typeof row.runId !== "string" ||
        typeof row.projectId !== "string" ||
        typeof row.ordinal !== "number" ||
        typeof row.step !== "string"
      )
        return yield* error(
          projectId ?? ("run-once-recovery" as ProjectId),
          null,
          null,
          "projection-corrupt",
        );
      const publication = {
        publicationId: row.publicationId,
        runId: AgentControlRunOnceId.make(row.runId),
        projectId: row.projectId as ProjectId,
        ordinal: row.ordinal,
        step: row.step as AgentControlRunOnceStep,
      } satisfies AgentControlRunOnceCommittedPublication;
      const observation = {
        projectId: publication.projectId,
        runId: publication.runId,
        ordinal: publication.ordinal,
        step: publication.step,
      } as const;
      yield* hooks.beforePublication(observation);
      yield* PubSub.publish(publications, publication);
      yield* hooks.afterPublication(observation);
      const publishedAt = DateTime.formatIso(yield* DateTime.now);
      const updated = yield* sql<{ readonly publicationId: unknown }>`
        UPDATE main.agent_control_run_once_publications
        SET published_at = ${publishedAt}, attempt_count = attempt_count + 1
        WHERE publication_id = ${publication.publicationId} AND published_at IS NULL
        RETURNING publication_id AS "publicationId"
      `.pipe(
        Effect.mapError((cause) =>
          error(publication.projectId, publication.runId, publication.step, "persistence", cause),
        ),
      );
      if (updated.length !== 1 || updated[0]?.publicationId !== publication.publicationId) {
        return yield* error(
          publication.projectId,
          publication.runId,
          publication.step,
          "authority-conflict",
        );
      }
    }
  });

  const commitStep = Effect.fn("AgentControlRunOnce.commitStep")(function* (
    run: PersistedRunState,
    step: AgentControlRunOnceStep,
    payload: JsonValue,
    bindings: RunOnceStepBindings,
    state: RunOnceStateBinding,
    recordedAt: string,
  ) {
    const result = yield* writeRunOnceStep(sql, {
      runId: run.runId,
      projectId: run.projectId,
      ordinal: run.nextOrdinal,
      step,
      payload,
      bindings,
      state,
      recordedAt,
    });
    if (!result.replayed) {
      yield* hooks.afterStepCommitted({
        projectId: run.projectId,
        runId: run.runId,
        ordinal: run.nextOrdinal,
        step,
      });
      yield* publishPending(run.projectId);
    }
  });

  const nextState = (
    run: PersistedRunState,
    patch: Partial<RunOnceStateBinding>,
  ): RunOnceStateBinding => ({
    projectId: run.projectId,
    status: "active",
    taskId: run.taskId,
    stageRunId: run.stageRunId,
    leaseId: run.leaseId,
    worktreeReservationId: run.worktreeReservationId,
    controlledThreadReservationId: run.controlledThreadReservationId,
    terminalTaskEventId: run.terminalTaskEventId,
    activationProjectRevision: run.activationProjectRevision,
    resetProjectRevision: run.resetProjectRevision,
    ...patch,
  });

  const processSerialized = Effect.fn("AgentControlRunOnce.processSerialized")(function* (
    projectId: ProjectId,
  ) {
    yield* publishPending(projectId);
    let run = yield* loadRunState(projectId);
    if (run === null) {
      const project = yield* readProjectHistory(projectId);
      if (project.state.mode !== "run-once") return;
      const admitted = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // The history replay, activation claim and first durable boundary share
            // one native SQLite snapshot. A concurrent source/task/mode write can
            // therefore only make the transaction fail; it cannot be admitted
            // from a mixed authority view.
            const authority = yield* newActivationAuthority(projectId);
            const activation = yield* admitRunOnceActivation(sql, authority.activation);
            const initial: PersistedRunState = {
              runId: authority.activation.runId,
              projectId,
              status: "active",
              nextOrdinal: 1,
              lastStep: "activation-admitted",
              taskId: null,
              stageRunId: null,
              leaseId: null,
              worktreeReservationId: null,
              controlledThreadReservationId: null,
              terminalTaskEventId: null,
              activationProjectRevision: authority.activation.activationEventStreamVersion,
              resetProjectRevision: null,
            };
            const step = yield* writeRunOnceStepInTransaction(sql, {
              runId: initial.runId,
              projectId,
              ordinal: initial.nextOrdinal,
              step: "activation-admitted",
              payload: {
                schemaVersion: 1,
                activation: authority.activation as unknown as JsonValue,
              },
              bindings: {},
              state: nextState(initial, {}),
              recordedAt: authority.activation.activatedAt,
            });
            return {
              activation: authority.activation,
              activationReplay: activation.replayed,
              step,
            };
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            cause instanceof AgentControlRunOnceError
              ? cause
              : error(projectId, null, "activation-admitted", "persistence", cause),
          ),
        );
      if (!admitted.activationReplay) {
        yield* hooks.afterActivationAuthority({
          projectId,
          runId: admitted.activation.runId,
          ordinal: 1,
          step: "activation-admitted",
        });
      }
      if (!admitted.step.replayed) {
        yield* hooks.afterStepCommitted({
          projectId,
          runId: admitted.activation.runId,
          ordinal: 1,
          step: "activation-admitted",
        });
      }
      yield* publishPending(projectId);
      run = yield* loadRunState(projectId);
      if (run === null) {
        const terminal = yield* sql<{ readonly status: unknown }>`
          SELECT status FROM main.agent_control_run_once_states
          WHERE run_id = ${admitted.activation.runId} AND project_id = ${projectId}
        `.pipe(
          Effect.mapError((cause) =>
            error(projectId, admitted.activation.runId, null, "persistence", cause),
          ),
        );
        if (
          terminal.length === 1 &&
          (terminal[0]?.status === "completed" || terminal[0]?.status === "no-eligible-task")
        )
          return;
        return yield* error(projectId, admitted.activation.runId, null, "projection-corrupt");
      }
    }

    while (run.status === "active") {
      const activation = yield* loadActivation(projectId, run.runId);
      const project = yield* readProjectHistory(projectId);
      const bindings: RunOnceStepBindings = {
        taskId: run.taskId,
        stageRunId: run.stageRunId,
        leaseId: run.leaseId,
        worktreeReservationId: run.worktreeReservationId,
        controlledThreadReservationId: run.controlledThreadReservationId,
        terminalTaskEventId: run.terminalTaskEventId,
      };
      if (project.state.mode === "paused" && project.state.pausedFromMode === "run-once") return;

      switch (run.lastStep) {
        case "activation-admitted": {
          if (project.state.mode !== "run-once") return;
          const selected = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                const authority = yield* newActivationAuthority(projectId, activation);
                if (!same(authority.activation, activation)) {
                  return yield* error(projectId, run!.runId, "task-selected", "authority-conflict");
                }
                const taskId = yield* selectAgentControlRunOnceCandidate(
                  sql,
                  projectId,
                  activation.githubIntakeSequence,
                );
                const expected =
                  [...authority.tasks]
                    .filter(
                      (task) =>
                        task.status === "candidate" &&
                        task.sourceGate === "eligible" &&
                        task.stage === "intake" &&
                        task.githubIntakeSequence === activation.githubIntakeSequence,
                    )
                    .sort(
                      (left, right) =>
                        left.source.issueNumber - right.source.issueNumber ||
                        left.taskId.localeCompare(right.taskId),
                    )[0]?.taskId ?? null;
                if (taskId !== expected) {
                  return yield* error(
                    projectId,
                    run!.runId,
                    "task-selected",
                    "task-history-corrupt",
                  );
                }
                const step = taskId === null ? "no-eligible-task" : "task-selected";
                const result = yield* writeRunOnceStepInTransaction(sql, {
                  runId: run!.runId,
                  projectId,
                  ordinal: run!.nextOrdinal,
                  step,
                  payload:
                    taskId === null
                      ? { schemaVersion: 1, outcome: "no-eligible-task" }
                      : { schemaVersion: 1, taskId },
                  bindings: taskId === null ? {} : { taskId },
                  state: nextState(run!, taskId === null ? {} : { taskId }),
                  recordedAt: activation.activatedAt,
                });
                return { result, step } as const;
              }),
            )
            .pipe(
              Effect.mapError((cause) =>
                cause instanceof AgentControlRunOnceError
                  ? cause
                  : error(projectId, run!.runId, "task-selected", "persistence", cause),
              ),
            );
          if (!selected.result.replayed) {
            yield* hooks.afterStepCommitted({
              projectId,
              runId: run.runId,
              ordinal: run.nextOrdinal,
              step: selected.step,
            });
          }
          yield* publishPending(projectId);
          break;
        }
        case "task-selected": {
          if (project.state.mode !== "run-once" || run.taskId === null) return;
          const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "stage-prepared");
          const result = yield* requireRunOnceMethod(
            stageRuns.prepareInitialForRunOnce,
            "AgentControlStageRun.prepareInitialForRunOnce",
          )(run.runId, { commandId, projectId, taskId: run.taskId as never }).pipe(
            Effect.mapError((cause) =>
              error(projectId, run!.runId, "stage-prepared", "downstream-rejected", cause),
            ),
          );
          const stage = result.state;
          yield* commitStep(
            run,
            "stage-prepared",
            { schemaVersion: 1, stageRunId: stage.stageRunId },
            { ...bindings, stageRunId: stage.stageRunId },
            nextState(run, { stageRunId: stage.stageRunId }),
            stage.updatedAt,
          );
          break;
        }
        case "stage-prepared": {
          if (project.state.mode !== "run-once" || run.taskId === null || run.stageRunId === null)
            return;
          const stage = yield* stageRuns.getStageRun({ projectId, taskId: run.taskId as never });
          const leaseId = yield* deriveAgentControlStageRunLeaseId({
            projectId,
            taskId: run.taskId as never,
          });
          const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "lease-reserved");
          const outcome = yield* requireRunOnceMethod(
            leases.dispatchControllerForRunOnce,
            "AgentControlStageRunLeaseEngine.dispatchControllerForRunOnce",
          )(run.runId, {
            type: "agentControl.stageRunLease.reserve",
            commandId,
            leaseId,
            projectId,
            taskId: stage.taskId,
            stageRunId: stage.stageRunId,
            attemptId: stage.attemptId,
            taskRevision: stage.taskRevision,
            githubIntakeSequence: stage.githubIntakeSequence,
            sourceIdentityFingerprint: stage.sourceIdentityFingerprint,
            fenceToken: 1,
            expectedRevision: 0,
            leaseDurationMs: LEASE_DURATION_MS,
          }).pipe(
            Effect.mapError((cause) =>
              error(projectId, run!.runId, "lease-reserved", "downstream-rejected", cause),
            ),
          );
          if (outcome._tag === "Rejected") {
            return yield* error(
              projectId,
              run.runId,
              "lease-reserved",
              "downstream-rejected",
              outcome.error,
            );
          }
          const lease = outcome.result.state;
          yield* commitStep(
            run,
            "lease-reserved",
            { schemaVersion: 1, leaseId },
            { ...bindings, leaseId },
            nextState(run, { leaseId }),
            lease.acquiredAt,
          );
          break;
        }
        case "lease-reserved": {
          if (project.state.mode !== "run-once" || run.taskId === null) return;
          const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "worktree-ready");
          const ready = yield* requireRunOnceMethod(
            worktrees.reserveAndMaterializeForRunOnce,
            "AgentControlWorktreeController.reserveAndMaterializeForRunOnce",
          )(run.runId, { commandId, projectId, taskId: run.taskId as never }).pipe(
            Effect.mapError((cause) =>
              error(projectId, run!.runId, "worktree-ready", "downstream-rejected", cause),
            ),
          );
          yield* commitStep(
            run,
            "worktree-ready",
            { schemaVersion: 1, reservationId: ready.reservationId },
            { ...bindings, worktreeReservationId: ready.reservationId },
            nextState(run, { worktreeReservationId: ready.reservationId }),
            ready.updatedAt,
          );
          break;
        }
        case "worktree-ready": {
          if (project.state.mode !== "run-once" || run.taskId === null) return;
          const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "thread-activated");
          const result = yield* requireRunOnceMethod(
            threads.activateInitialForRunOnce,
            "AgentControlControlledThreadActivation.activateInitialForRunOnce",
          )(run.runId, { commandId, projectId, taskId: run.taskId as never }).pipe(
            Effect.mapError((cause) =>
              error(projectId, run!.runId, "thread-activated", "downstream-rejected", cause),
            ),
          );
          const reservationId = result.reservation.controlledThreadReservationId;
          yield* commitStep(
            run,
            "thread-activated",
            { schemaVersion: 1, controlledThreadReservationId: reservationId },
            { ...bindings, controlledThreadReservationId: reservationId },
            nextState(run, { controlledThreadReservationId: reservationId }),
            result.reservation.preparedAt,
          );
          break;
        }
        case "thread-activated": {
          if (run.taskId === null)
            return yield* error(projectId, run.runId, run.lastStep, "projection-corrupt");
          const events = yield* taskEvents
            .readStream(run.taskId as never, 0, 10_000)
            .pipe(
              Effect.mapError((cause) =>
                error(projectId, run!.runId, "task-terminal-observed", "persistence", cause),
              ),
            );
          const terminal = yield* loadRunOnceTerminalAuthority(
            sql,
            projectId,
            run.runId,
            run.taskId as never,
            events,
          );
          if (terminal === null) return;
          yield* commitStep(
            run,
            "task-terminal-observed",
            {
              schemaVersion: 1,
              status: terminal.status,
              taskFinalizationEvidenceId: terminal.taskFinalizationEvidenceId,
            },
            {
              ...bindings,
              terminalTaskEventId: terminal.event.eventId,
              terminalTaskEventSequence: terminal.event.sequence,
              terminalTaskEventStreamVersion: terminal.event.streamVersion,
            },
            nextState(run, { terminalTaskEventId: terminal.event.eventId }),
            terminal.event.occurredAt,
          );
          break;
        }
        case "no-eligible-task":
        case "task-terminal-observed": {
          if (project.state.mode === "run-once") {
            const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "mode-reset");
            const result = yield* projectEngine
              .dispatchSystem({
                commandId,
                projectId,
                expectedRevision: project.state.revision,
                mode: "observe",
              })
              .pipe(
                Effect.mapError((cause) =>
                  error(projectId, run!.runId, "mode-reset", "mode-superseded", cause),
                ),
              );
            const history = yield* readProjectHistory(projectId);
            const event = history.events.find((candidate) => candidate.commandId === commandId);
            if (
              event === undefined ||
              event.authority !== "system" ||
              event.type !== "agentControl.project.mode.changed" ||
              event.payload.previousMode !== "run-once" ||
              event.payload.mode !== "observe" ||
              event.payload.previousPausedFromMode !== null ||
              event.payload.pausedFromMode !== null ||
              event.streamVersion !== result.state.revision ||
              event.sequence !== result.state.sequence
            ) {
              return yield* error(projectId, run.runId, "mode-reset", "authority-conflict");
            }
            yield* commitStep(
              run,
              "mode-reset",
              { schemaVersion: 1, projectRevision: event.streamVersion },
              {
                ...bindings,
                modeEventId: event.eventId,
                modeEventSequence: event.sequence,
                modeEventStreamVersion: event.streamVersion,
              },
              nextState(run, { resetProjectRevision: event.streamVersion }),
              event.occurredAt,
            );
          } else if (project.state.mode === "manual") {
            yield* commitStep(
              run,
              "mode-reset-superseded",
              { schemaVersion: 1, projectRevision: project.state.revision },
              bindings,
              nextState(run, { resetProjectRevision: project.state.revision }),
              project.state.updatedAt ?? activation.activatedAt,
            );
          } else {
            return;
          }
          break;
        }
        case "mode-reset":
        case "mode-reset-superseded": {
          const finalStatus = run.taskId === null ? "no-eligible-task" : "completed";
          yield* commitStep(
            run,
            "completed",
            { schemaVersion: 1, status: finalStatus },
            bindings,
            nextState(run, { status: finalStatus }),
            project.state.updatedAt ?? activation.activatedAt,
          );
          return;
        }
        case "completed":
          return;
      }
      const next = yield* loadRunState(projectId);
      if (next === null) return;
      run = next;
    }
  });

  const processProject: AgentControlRunOnceControllerShape["processProject"] = (projectId) =>
    getProjectLock(projectId).pipe(
      Effect.flatMap((lock) => lock.withPermit(processSerialized(projectId))),
      Effect.mapError((cause) =>
        cause instanceof AgentControlRunOnceError
          ? cause
          : error(projectId, null, null, "persistence", cause),
      ),
    );

  const recover: AgentControlRunOnceControllerShape["recover"] = Effect.gen(function* () {
    yield* publishPending();
    const rows = yield* sql<{ readonly projectId: unknown }>`
      SELECT project_id AS "projectId" FROM main.agent_control_run_once_states
      WHERE status = 'active' ORDER BY project_id
    `.pipe(
      Effect.mapError((cause) =>
        error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
      ),
    );
    for (const row of rows) {
      if (typeof row.projectId !== "string") {
        return yield* error("run-once-recovery" as ProjectId, null, null, "projection-corrupt");
      }
      yield* processProject(row.projectId as ProjectId);
    }
  });

  const prepare: AgentControlRunOnceControllerShape["prepare"] = (activation) =>
    Effect.gen(function* () {
      const projectStream = yield* requireRunOnceMethod(
        projectEngine.subscribeDomainEvents,
        "AgentControlEngine.subscribeDomainEvents",
      );
      const taskStream = yield* taskEngine.subscribeDomainEvents;
      yield* hooks.afterSubscriptionsBeforeRecovery;
      yield* activation.await;
      yield* recover;
      yield* Stream.runForEach(projectStream, (event) =>
        processProject(event.aggregateId).pipe(Effect.ignoreCause({ log: true })),
      ).pipe(Effect.forkScoped);
      yield* Stream.runForEach(taskStream, (event) =>
        event.type === "agentControl.task.finalizedAfterVerification"
          ? processProject(event.payload.projectId).pipe(Effect.ignoreCause({ log: true }))
          : Effect.void,
      ).pipe(Effect.forkScoped);
    }).pipe(Effect.ignoreCause({ log: true }));

  return AgentControlRunOnceController.of({
    recover,
    processProject,
    prepare,
    subscribePublications: PubSub.subscribe(publications).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const AgentControlRunOnceControllerLive = Layer.effect(AgentControlRunOnceController, make);
