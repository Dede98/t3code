import {
  AgentControlRunOnceId,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  type AgentControlProjectState,
  type AgentControlRunOnceActivation,
  type AgentControlRunOnceStep,
  type AgentControlTaskEvent,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
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
  loadRunOnceModeAuthority,
  loadRunOnceTerminalAuthority,
  writeRunOnceStep,
  writeRunOnceStepInTransaction,
  type RunOnceStateBinding,
  type RunOnceModeAuthority,
} from "../authority.ts";
import { deriveAgentControlRunOnceId, deriveRunOnceCommandId } from "../identity.ts";
import { AgentControlRunOnceError, type RunOnceStepBindings } from "../model.ts";
import {
  isAgentControlRunOnceCandidateVacant,
  selectAgentControlRunOnceCandidate,
} from "../selection.ts";
import { fingerprintAgentControlRunOnceSource } from "../source.ts";
import {
  AgentControlRunOnceController,
  type AgentControlRunOnceCommittedPublication,
  type AgentControlRunOnceControllerShape,
} from "../Services/AgentControlRunOnceController.ts";
import { AgentControlRunOnceControllerHooks } from "../Services/AgentControlRunOnceControllerHooks.ts";
import { makeAgentControlRunOnceKeyedFence, requireRunOnceMethod } from "../context.ts";

const PAGE_SIZE = 500;
const LEASE_DURATION_MS = 60_000;
interface PersistedRunState extends RunOnceStateBinding {
  readonly runId: AgentControlRunOnceId;
  readonly nextOrdinal: number;
  readonly lastStep: AgentControlRunOnceStep;
  readonly terminalTaskEventSequence: number | null;
  readonly terminalTaskEventStreamVersion: number | null;
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

export const readFullRunOnceTaskHistory = Effect.fn("readFullRunOnceTaskHistory")(function* (
  taskEvents: AgentControlTaskEventStore["Service"],
  projectId: ProjectId,
  runId: AgentControlRunOnceId,
  taskId: AgentControlTaskId,
) {
  const all: Array<AgentControlTaskEvent> = [];
  let cursor = 0;
  while (true) {
    const page = yield* taskEvents
      .readStream(taskId, cursor, PAGE_SIZE)
      .pipe(
        Effect.mapError((cause) =>
          error(projectId, runId, "task-terminal-observed", "persistence", cause),
        ),
      );
    if (page.length === 0) break;
    for (const event of page) {
      if (
        event.aggregateKind !== "task" ||
        event.aggregateId !== taskId ||
        event.streamVersion !== cursor + 1
      ) {
        return yield* error(projectId, runId, "task-terminal-observed", "task-history-corrupt");
      }
      all.push(event);
      cursor = event.streamVersion;
    }
  }
  return all;
});

const same = (left: unknown, right: unknown) =>
  canonicalJson(left as JsonValue) === canonicalJson(right as JsonValue);
const sameBytes = (left: Uint8Array, right: Uint8Array) =>
  left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);

export const superviseAgentControlRunOnceListener = <A>(
  initialStream: Stream.Stream<A>,
  subscribe: Effect.Effect<Stream.Stream<A>, never, Scope.Scope>,
  onEvent: (event: A) => Effect.Effect<void>,
  name: "project" | "task",
): Effect.Effect<void, never, Scope.Scope> => {
  let initial: Stream.Stream<A> | undefined = initialStream;
  let consecutiveFailures = 0;
  const loop = (): Effect.Effect<void> =>
    Effect.suspend(() => {
      const consume = initial === undefined ? subscribe : Effect.succeed(initial);
      initial = undefined;
      return Effect.scoped(
        consume.pipe(
          Effect.flatMap((stream) =>
            Stream.runForEach(stream, (event) =>
              onEvent(event).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    consecutiveFailures = 0;
                  }),
                ),
              ),
            ),
          ),
        ),
      ).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) =>
            Effect.sync(() => {
              consecutiveFailures += 1;
              return consecutiveFailures;
            }).pipe(
              Effect.flatMap((failures) =>
                Effect.logError("Run-Once listener restarting", {
                  listener: name,
                  cause,
                  consecutiveFailures: failures,
                }).pipe(
                  Effect.andThen(
                    failures === 1
                      ? Effect.yieldNow
                      : Effect.sleep(
                          Duration.millis(Math.min(1_000, 25 * 2 ** Math.min(5, failures - 2))),
                        ),
                  ),
                  Effect.andThen(loop()),
                ),
              ),
            ),
        ),
      );
    });
  return loop().pipe(Effect.forkScoped, Effect.asVoid);
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
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
  const publicationConsumerOwner = yield* crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) =>
      error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
    ),
  );
  const projectLocks = makeAgentControlRunOnceKeyedFence<ProjectId>();

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
      SELECT state.run_id AS "runId", state.project_id AS "projectId", state.status,
        state.next_ordinal AS "nextOrdinal", state.last_step AS "lastStep",
        state.task_id AS "taskId", state.stage_run_id AS "stageRunId",
        state.lease_id AS "leaseId",
        state.worktree_reservation_id AS "worktreeReservationId",
        state.controlled_thread_reservation_id AS "controlledThreadReservationId",
        state.terminal_task_event_id AS "terminalTaskEventId",
        evidence.terminal_task_event_sequence AS "terminalTaskEventSequence",
        evidence.terminal_task_event_stream_version AS "terminalTaskEventStreamVersion",
        state.activation_project_revision AS "activationProjectRevision",
        state.reset_project_revision AS "resetProjectRevision"
      FROM main.agent_control_run_once_states state
      JOIN main.agent_control_run_once_step_evidence evidence
        ON evidence.run_id = state.run_id
        AND evidence.ordinal = state.next_ordinal - 1
        AND evidence.step = state.last_step
        AND evidence.terminal_task_event_id IS state.terminal_task_event_id
      JOIN main.agent_control_run_once_step_markers marker
        ON marker.evidence_id = evidence.evidence_id
      WHERE state.project_id = ${projectId} AND state.status = 'active'
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
    const terminalTaskEventId =
      typeof row.terminalTaskEventId === "string" ? row.terminalTaskEventId : null;
    const terminalTaskEventSequence =
      typeof row.terminalTaskEventSequence === "number" &&
      Number.isSafeInteger(row.terminalTaskEventSequence) &&
      row.terminalTaskEventSequence >= 1
        ? row.terminalTaskEventSequence
        : null;
    const terminalTaskEventStreamVersion =
      typeof row.terminalTaskEventStreamVersion === "number" &&
      Number.isSafeInteger(row.terminalTaskEventStreamVersion) &&
      row.terminalTaskEventStreamVersion >= 1
        ? row.terminalTaskEventStreamVersion
        : null;
    if (
      (terminalTaskEventId === null) !== (terminalTaskEventSequence === null) ||
      (terminalTaskEventId === null) !== (terminalTaskEventStreamVersion === null)
    ) {
      return yield* error(projectId, null, null, "projection-corrupt");
    }
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
      terminalTaskEventId,
      terminalTaskEventSequence,
      terminalTaskEventStreamVersion,
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
        activation_expected_revision AS "activationExpectedRevision",
        activation_command_fingerprint AS "activationCommandFingerprint",
        activation_event_payload_json AS "activationEventPayloadBytes",
        activation_event_metadata_json AS "activationEventMetadataBytes",
        github_intake_sequence AS "githubIntakeSequence", github_event_id AS "githubEventId",
        github_event_sequence AS "githubEventSequence",
        github_event_stream_version AS "githubEventStreamVersion",
        reconcile_revision AS "reconcileRevision", source_fingerprint AS "sourceFingerprint",
        activated_at AS "activatedAt"
      FROM main.agent_control_run_once_activations WHERE run_id = ${runId}
    `.pipe(Effect.mapError((cause) => error(projectId, runId, null, "persistence", cause)));
    const row = rows[0];
    if (
      rows.length !== 1 ||
      row === undefined ||
      typeof row.activationExpectedRevision !== "number" ||
      typeof row.activationCommandFingerprint !== "string" ||
      !(row.activationEventPayloadBytes instanceof Uint8Array) ||
      !(row.activationEventMetadataBytes instanceof Uint8Array)
    ) {
      return yield* error(projectId, runId, null, "authority-conflict");
    }
    const activation = {
      schemaVersion: 1,
      runId: row.runId,
      projectId: row.projectId,
      activationEventId: row.activationEventId,
      activationEventSequence: row.activationEventSequence,
      activationEventStreamVersion: row.activationEventStreamVersion,
      activationCommandId: row.activationCommandId,
      githubIntakeSequence: row.githubIntakeSequence,
      githubEventId: row.githubEventId,
      githubEventSequence: row.githubEventSequence,
      githubEventStreamVersion: row.githubEventStreamVersion,
      reconcileRevision: row.reconcileRevision,
      sourceFingerprint: row.sourceFingerprint,
      activatedAt: row.activatedAt,
    } as AgentControlRunOnceActivation;
    return {
      activation,
      modeAuthority: {
        expectedRevision: row.activationExpectedRevision,
        commandFingerprint: row.activationCommandFingerprint,
        eventPayloadBytes: row.activationEventPayloadBytes,
        eventMetadataBytes: row.activationEventMetadataBytes,
      } satisfies RunOnceModeAuthority,
    } as const;
  });

  const newActivationAuthority = Effect.fn("AgentControlRunOnce.newActivationAuthority")(function* (
    projectId: ProjectId,
    existingActivation?: {
      readonly activation: AgentControlRunOnceActivation;
      readonly modeAuthority: RunOnceModeAuthority;
    },
  ) {
    const project = yield* readProjectHistory(projectId);
    const activationEvent =
      existingActivation === undefined
        ? project.events.at(-1)
        : project.events.find(
            (event) =>
              event.eventId === existingActivation.activation.activationEventId &&
              event.sequence === existingActivation.activation.activationEventSequence &&
              event.streamVersion === existingActivation.activation.activationEventStreamVersion &&
              event.commandId === existingActivation.activation.activationCommandId,
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
            existingActivation.activation.runId,
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
          existingActivation.activation.runId,
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
    const modeAuthority = yield* loadRunOnceModeAuthority(sql, projectId, activationEvent);
    if (
      existingActivation !== undefined &&
      (existingActivation.modeAuthority.expectedRevision !== modeAuthority.expectedRevision ||
        existingActivation.modeAuthority.commandFingerprint !== modeAuthority.commandFingerprint ||
        !sameBytes(
          existingActivation.modeAuthority.eventPayloadBytes,
          modeAuthority.eventPayloadBytes,
        ) ||
        !sameBytes(
          existingActivation.modeAuthority.eventMetadataBytes,
          modeAuthority.eventMetadataBytes,
        ))
    ) {
      return yield* error(projectId, runId, "activation-admitted", "authority-conflict");
    }
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
      modeAuthority,
      tasks,
    } as const;
  });

  const inspectActivationLineage = (
    project: {
      readonly events: ReadonlyArray<AgentControlEvent>;
      readonly state: AgentControlProjectState;
    },
    activation: AgentControlRunOnceActivation,
    run: PersistedRunState,
  ) => {
    let mode: "run-once" | "paused" = "run-once";
    for (const event of project.events.filter(
      (candidate) => candidate.streamVersion > activation.activationEventStreamVersion,
    )) {
      const pause: boolean =
        mode === "run-once" &&
        event.authority === "human" &&
        event.payload.previousMode === "run-once" &&
        event.payload.mode === "paused" &&
        event.payload.previousPausedFromMode === null &&
        event.payload.pausedFromMode === "run-once";
      const resume: boolean =
        mode === "paused" &&
        event.authority === "human" &&
        event.payload.previousMode === "paused" &&
        event.payload.mode === "run-once" &&
        event.payload.previousPausedFromMode === "run-once" &&
        event.payload.pausedFromMode === null;
      if (pause || resume) {
        mode = pause ? "paused" : "run-once";
        continue;
      }

      const expectedResetCommandId = deriveRunOnceCommandId(
        run.runId,
        run.nextOrdinal,
        "mode-reset",
      );
      if (
        (run.lastStep === "no-eligible-task" || run.lastStep === "task-terminal-observed") &&
        mode === "run-once" &&
        event.authority === "system" &&
        event.commandId === expectedResetCommandId &&
        event.correlationId === expectedResetCommandId &&
        event.causationEventId === null &&
        event.payload.previousMode === "run-once" &&
        event.payload.mode === "observe" &&
        event.payload.previousPausedFromMode === null &&
        event.payload.pausedFromMode === null
      ) {
        return { _tag: "reset" as const, event };
      }

      if (
        event.authority === "human" &&
        event.payload.previousMode === mode &&
        event.payload.previousPausedFromMode === (mode === "paused" ? "run-once" : null) &&
        (event.payload.mode === "manual" || event.payload.mode === "observe") &&
        event.payload.pausedFromMode === null
      ) {
        return { _tag: "superseded" as const, event };
      }
      return { _tag: "invalid" as const };
    }

    const currentMatches =
      (mode === "run-once" &&
        project.state.mode === "run-once" &&
        project.state.pausedFromMode === null) ||
      (mode === "paused" &&
        project.state.mode === "paused" &&
        project.state.pausedFromMode === "run-once");
    return currentMatches
      ? { _tag: "current" as const, paused: mode === "paused" }
      : { _tag: "invalid" as const };
  };

  const loadPublications = Effect.fn("AgentControlRunOnce.loadPublications")(function* (
    published: boolean,
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
      WHERE (${published ? 1 : 0} = 1) = (publication.published_at IS NOT NULL)
        AND (${projectId ?? null} IS NULL OR activation.project_id = ${projectId ?? null})
      ORDER BY publication.run_id, publication.ordinal
    `.pipe(
      Effect.mapError((cause) =>
        error(projectId ?? ("run-once-recovery" as ProjectId), null, null, "persistence", cause),
      ),
    );
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        if (
          typeof row.publicationId !== "string" ||
          typeof row.runId !== "string" ||
          typeof row.projectId !== "string" ||
          typeof row.ordinal !== "number" ||
          typeof row.step !== "string"
        ) {
          return yield* error(
            projectId ?? ("run-once-recovery" as ProjectId),
            null,
            null,
            "projection-corrupt",
          );
        }
        return {
          publicationId: row.publicationId,
          runId: AgentControlRunOnceId.make(row.runId),
          projectId: row.projectId as ProjectId,
          ordinal: row.ordinal,
          step: row.step as AgentControlRunOnceStep,
        } satisfies AgentControlRunOnceCommittedPublication;
      }),
    );
  });

  const wakePublication = Effect.fn("AgentControlRunOnce.wakePublication")(function* (
    publication: AgentControlRunOnceCommittedPublication,
  ) {
    const observation = {
      projectId: publication.projectId,
      runId: publication.runId,
      ordinal: publication.ordinal,
      step: publication.step,
    } as const;
    yield* hooks.beforePublication(observation);
    yield* PubSub.publish(publications, publication);
    yield* hooks.afterPublication(observation);
  });

  const publishPending = Effect.fn("AgentControlRunOnce.publishPending")(function* (
    projectId?: ProjectId,
  ) {
    const rows = yield* loadPublications(false, projectId);
    for (const publication of rows) {
      const publishedAt = DateTime.formatIso(yield* DateTime.now);
      const claimed = yield* sql<{ readonly publicationId: unknown }>`
        UPDATE main.agent_control_run_once_publications
        SET published_at = ${publishedAt}, attempt_count = attempt_count + 1
        WHERE publication_id = ${publication.publicationId}
          AND published_at IS NULL
        RETURNING publication_id AS "publicationId"
      `.pipe(
        Effect.mapError((cause) =>
          error(publication.projectId, publication.runId, publication.step, "persistence", cause),
        ),
      );
      if (claimed.length === 0) continue;
      if (claimed.length !== 1 || claimed[0]?.publicationId !== publication.publicationId) {
        return yield* error(
          publication.projectId,
          publication.runId,
          publication.step,
          "authority-conflict",
        );
      }
      yield* hooks.beforePublication({
        projectId: publication.projectId,
        runId: publication.runId,
        ordinal: publication.ordinal,
        step: publication.step,
      });
      yield* PubSub.publish(publications, publication);
      yield* hooks.afterPublication({
        projectId: publication.projectId,
        runId: publication.runId,
        ordinal: publication.ordinal,
        step: publication.step,
      });
    }
  });

  const recoverPublications = Effect.fn("AgentControlRunOnce.recoverPublications")(function* () {
    // A PubSub item is only an idempotent wake-up for the durable outbox row.
    // Re-waking rows that were already scheduled closes both crash windows:
    // after the durable CAS but before PubSub, and immediately after PubSub.
    const committedBeforeRecovery = yield* loadPublications(true);
    yield* publishPending();
    yield* Effect.forEach(committedBeforeRecovery, wakePublication, { discard: true });
  });

  const auditRecoveryAuthority = Effect.fn("AgentControlRunOnce.auditRecoveryAuthority")(
    function* () {
      const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
      const integrity = yield* sql<{ readonly integrity_check: unknown }>`
        PRAGMA integrity_check
      `;
      const invalidActivations = yield* sql<{ readonly runId: unknown }>`
        SELECT activation.run_id AS "runId"
        FROM main.agent_control_run_once_activations activation
        WHERE NOT EXISTS (
          SELECT 1
          FROM main.agent_control_events event
          JOIN main.agent_control_command_receipts receipt
            ON receipt.command_id = event.command_id
          WHERE event.event_id = activation.activation_event_id
            AND event.aggregate_kind = 'project-controller'
            AND event.stream_id = activation.project_id
            AND event.event_type = 'agentControl.project.mode.changed'
            AND event.actor_authority = 'human'
            AND event.sequence = activation.activation_event_sequence
            AND event.stream_version = activation.activation_event_stream_version
            AND event.command_id = activation.activation_command_id
            AND event.correlation_id = activation.activation_command_id
            AND event.causation_event_id IS NULL
            AND event.occurred_at = activation.activated_at
            AND CAST(event.payload_json AS BLOB) = activation.activation_event_payload_json
            AND CAST(event.metadata_json AS BLOB) = activation.activation_event_metadata_json
            AND receipt.authority = 'human'
            AND receipt.aggregate_kind = 'project-controller'
            AND receipt.aggregate_id = activation.project_id
            AND receipt.status = 'accepted'
            AND receipt.event_created = 1
            AND receipt.result_sequence = event.sequence
            AND receipt.result_stream_version = event.stream_version
            AND receipt.accepted_at = event.occurred_at
            AND receipt.error_code IS NULL
            AND receipt.command_fingerprint = activation.activation_command_fingerprint
        )
        LIMIT 1
      `;
      const invalidStates = yield* sql<{ readonly runId: unknown }>`
        SELECT state.run_id AS "runId"
        FROM main.agent_control_run_once_states state
        WHERE NOT EXISTS (
          SELECT 1
          FROM main.agent_control_run_once_activations activation
          JOIN main.agent_control_run_once_step_evidence evidence
            ON evidence.run_id = state.run_id
           AND evidence.ordinal = state.next_ordinal - 1
           AND evidence.step = state.last_step
          JOIN main.agent_control_run_once_step_receipts receipt
            ON receipt.evidence_id = evidence.evidence_id
          JOIN main.agent_control_run_once_step_markers marker
            ON marker.evidence_id = evidence.evidence_id
           AND marker.receipt_id = receipt.receipt_id
          JOIN main.agent_control_run_once_publications publication
            ON publication.evidence_id = evidence.evidence_id
           AND publication.marker_id = marker.marker_id
          WHERE activation.run_id = state.run_id
            AND activation.project_id = state.project_id
            AND activation.activation_event_stream_version = state.activation_project_revision
            AND evidence.task_id IS state.task_id
            AND evidence.stage_run_id IS state.stage_run_id
            AND evidence.lease_id IS state.lease_id
            AND evidence.worktree_reservation_id IS state.worktree_reservation_id
            AND evidence.controlled_thread_reservation_id IS state.controlled_thread_reservation_id
            AND evidence.terminal_task_event_id IS state.terminal_task_event_id
        )
        LIMIT 1
      `;
      if (
        foreignKeys.length !== 0 ||
        integrity.length !== 1 ||
        integrity[0]?.integrity_check !== "ok" ||
        invalidActivations.length !== 0 ||
        invalidStates.length !== 0
      ) {
        return yield* error("run-once-recovery" as ProjectId, null, null, "projection-corrupt");
      }
    },
  );

  const recoverPublicationConsumer: AgentControlRunOnceControllerShape["recoverPublicationConsumer"] =
    Effect.fn("AgentControlRunOnce.recoverPublicationConsumer")(function* (rawConsumerId) {
      if (rawConsumerId.length === 0) {
        return yield* error("run-once-recovery" as ProjectId, null, null, "authority-conflict");
      }
      const consumerId = rawConsumerId;
      yield* sql`
        UPDATE main.agent_control_run_once_publication_inbox
        SET claim_owner = NULL, claimed_at = NULL
        WHERE consumer_id = ${consumerId}
          AND acknowledged_at IS NULL AND claim_owner IS NOT NULL
      `.pipe(
        Effect.mapError((cause) =>
          error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
        ),
      );
    });

  const pullPublications: AgentControlRunOnceControllerShape["pullPublications"] = Effect.fn(
    "AgentControlRunOnce.pullPublications",
  )(function* (rawConsumerId) {
    if (rawConsumerId.length === 0) {
      return yield* error("run-once-recovery" as ProjectId, null, null, "authority-conflict");
    }
    const consumerId = rawConsumerId;
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO main.agent_control_run_once_publication_inbox (
              consumer_id, publication_id, run_id, ordinal, step
            )
            SELECT ${consumerId}, publication.publication_id, publication.run_id,
              publication.ordinal, publication.step
            FROM main.agent_control_run_once_publications publication
            JOIN main.agent_control_run_once_step_markers marker
              ON marker.marker_id = publication.marker_id
            WHERE publication.published_at IS NOT NULL
            ORDER BY publication.run_id, publication.ordinal
            ON CONFLICT (consumer_id, publication_id) DO NOTHING
          `;
          const pending = yield* sql<Record<string, unknown>>`
            SELECT publication_id AS "publicationId", run_id AS "runId", ordinal, step
            FROM main.agent_control_run_once_publication_inbox
            WHERE consumer_id = ${consumerId}
              AND acknowledged_at IS NULL AND claim_owner IS NULL
            ORDER BY run_id, ordinal
          `;
          const claimedAt = DateTime.formatIso(yield* DateTime.now);
          const claimed: Array<AgentControlRunOnceCommittedPublication> = [];
          for (const row of pending) {
            if (
              typeof row.publicationId !== "string" ||
              typeof row.runId !== "string" ||
              typeof row.ordinal !== "number" ||
              typeof row.step !== "string"
            ) {
              return yield* error(
                "run-once-recovery" as ProjectId,
                null,
                null,
                "projection-corrupt",
              );
            }
            const updated = yield* sql<{ readonly publicationId: unknown }>`
              UPDATE main.agent_control_run_once_publication_inbox
              SET claim_owner = ${publicationConsumerOwner}, claimed_at = ${claimedAt},
                delivery_count = delivery_count + 1
              WHERE consumer_id = ${consumerId} AND publication_id = ${row.publicationId}
                AND acknowledged_at IS NULL AND claim_owner IS NULL
              RETURNING publication_id AS "publicationId"
            `;
            if (updated.length === 0) continue;
            if (updated.length !== 1 || updated[0]?.publicationId !== row.publicationId) {
              return yield* error(
                "run-once-recovery" as ProjectId,
                null,
                null,
                "authority-conflict",
              );
            }
            const projectRows = yield* sql<{ readonly projectId: unknown }>`
              SELECT activation.project_id AS "projectId"
              FROM main.agent_control_run_once_activations activation
              WHERE activation.run_id = ${row.runId}
            `;
            if (projectRows.length !== 1 || typeof projectRows[0]?.projectId !== "string") {
              return yield* error(
                "run-once-recovery" as ProjectId,
                null,
                null,
                "projection-corrupt",
              );
            }
            claimed.push({
              publicationId: row.publicationId,
              runId: AgentControlRunOnceId.make(row.runId),
              projectId: projectRows[0].projectId as ProjectId,
              ordinal: row.ordinal,
              step: row.step as AgentControlRunOnceStep,
            });
          }
          return claimed;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          cause instanceof AgentControlRunOnceError
            ? cause
            : error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
        ),
      );
  });

  const acknowledgePublication: AgentControlRunOnceControllerShape["acknowledgePublication"] =
    Effect.fn("AgentControlRunOnce.acknowledgePublication")(
      function* (rawConsumerId, publicationId) {
        if (rawConsumerId.length === 0) {
          return yield* error("run-once-recovery" as ProjectId, null, null, "authority-conflict");
        }
        const consumerId = rawConsumerId;
        const acknowledgedAt = DateTime.formatIso(yield* DateTime.now);
        const updated = yield* sql<{ readonly publicationId: unknown }>`
        UPDATE main.agent_control_run_once_publication_inbox
        SET claim_owner = NULL, claimed_at = NULL, acknowledged_at = ${acknowledgedAt}
        WHERE consumer_id = ${consumerId} AND publication_id = ${publicationId}
          AND acknowledged_at IS NULL AND claim_owner = ${publicationConsumerOwner}
        RETURNING publication_id AS "publicationId"
      `.pipe(
          Effect.mapError((cause) =>
            error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
          ),
        );
        if (updated.length !== 1 || updated[0]?.publicationId !== publicationId) {
          return yield* error("run-once-recovery" as ProjectId, null, null, "authority-conflict");
        }
      },
    );

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

  const modeBindings = (
    bindings: RunOnceStepBindings,
    event: AgentControlEvent,
    authority: RunOnceModeAuthority,
  ): RunOnceStepBindings => ({
    ...bindings,
    modeEventId: event.eventId,
    modeEventSequence: event.sequence,
    modeEventStreamVersion: event.streamVersion,
    modeExpectedRevision: authority.expectedRevision,
    modeCommandFingerprint: authority.commandFingerprint,
    modeEventPayloadBytes: authority.eventPayloadBytes,
    modeEventMetadataBytes: authority.eventMetadataBytes,
  });

  const processSerialized: AgentControlRunOnceControllerShape["processProject"] = Effect.fn(
    "AgentControlRunOnce.processSerialized",
  )(function* (projectId: ProjectId) {
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
            const activation = yield* admitRunOnceActivation(
              sql,
              authority.activation,
              authority.modeAuthority,
            );
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
              terminalTaskEventSequence: null,
              terminalTaskEventStreamVersion: null,
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
      const loadedActivation = yield* loadActivation(projectId, run.runId);
      const activation = loadedActivation.activation;
      const project = yield* readProjectHistory(projectId);
      const bindings: RunOnceStepBindings = {
        taskId: run.taskId,
        stageRunId: run.stageRunId,
        leaseId: run.leaseId,
        worktreeReservationId: run.worktreeReservationId,
        controlledThreadReservationId: run.controlledThreadReservationId,
        terminalTaskEventId: run.terminalTaskEventId,
        terminalTaskEventSequence: run.terminalTaskEventSequence,
        terminalTaskEventStreamVersion: run.terminalTaskEventStreamVersion,
      };
      if (run.lastStep !== "mode-reset" && run.lastStep !== "mode-reset-superseded") {
        const lineage = inspectActivationLineage(project, activation, run);
        if (lineage._tag === "invalid") {
          return yield* error(projectId, run.runId, run.lastStep, "authority-conflict");
        }
        if (lineage._tag === "current" && lineage.paused) return;
        if (lineage._tag === "reset" || lineage._tag === "superseded") {
          const step = lineage._tag === "reset" ? "mode-reset" : "mode-reset-superseded";
          const authority = yield* loadRunOnceModeAuthority(sql, projectId, lineage.event);
          yield* commitStep(
            run,
            step,
            { schemaVersion: 1, projectRevision: lineage.event.streamVersion },
            modeBindings(bindings, lineage.event, authority),
            nextState(run, { resetProjectRevision: lineage.event.streamVersion }),
            lineage.event.occurredAt,
          );
          const next = yield* loadRunState(projectId);
          if (next === null) return;
          run = next;
          continue;
        }
      }

      switch (run.lastStep) {
        case "activation-admitted": {
          const selected = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                const authority = yield* newActivationAuthority(projectId, loadedActivation);
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
                if (
                  taskId !== null &&
                  !(yield* isAgentControlRunOnceCandidateVacant(sql, projectId, taskId))
                ) {
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
          if (run.taskId === null) return;
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
          if (run.taskId === null || run.stageRunId === null) return;
          const stage = yield* stageRuns
            .getStageRun({ projectId, taskId: run.taskId as never })
            .pipe(
              Effect.mapError((cause) =>
                error(projectId, run!.runId, "lease-reserved", "downstream-rejected", cause),
              ),
            );
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
          if (run.taskId === null) return;
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
          if (run.taskId === null) return;
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
          const events = yield* readFullRunOnceTaskHistory(
            taskEvents,
            projectId,
            run.runId,
            run.taskId as AgentControlTaskId,
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
          const commandId = deriveRunOnceCommandId(run.runId, run.nextOrdinal, "mode-reset");
          const dispatched = yield* Effect.exit(
            projectEngine.dispatchSystem({
              commandId,
              projectId,
              expectedRevision: project.state.revision,
              mode: "observe",
            }),
          );
          if (Exit.isFailure(dispatched)) {
            const raced = yield* readProjectHistory(projectId);
            if (raced.state.mode !== "run-once") continue;
            return yield* error(
              projectId,
              run.runId,
              "mode-reset",
              "mode-superseded",
              dispatched.cause,
            );
          }
          const result = dispatched.value;
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
          const authority = yield* loadRunOnceModeAuthority(sql, projectId, event);
          yield* commitStep(
            run,
            "mode-reset",
            { schemaVersion: 1, projectRevision: event.streamVersion },
            modeBindings(bindings, event, authority),
            nextState(run, { resetProjectRevision: event.streamVersion }),
            event.occurredAt,
          );
          break;
        }
        case "mode-reset":
        case "mode-reset-superseded": {
          const admitSuccessor = run.lastStep === "mode-reset-superseded";
          const noEligibleRows = yield* sql<{ readonly found: unknown }>`
            SELECT EXISTS (
              SELECT 1 FROM main.agent_control_run_once_step_evidence
              WHERE run_id = ${run.runId} AND step = 'no-eligible-task'
            ) AS found
          `.pipe(
            Effect.mapError((cause) =>
              error(projectId, run!.runId, "completed", "persistence", cause),
            ),
          );
          if (noEligibleRows.length !== 1 || typeof noEligibleRows[0]?.found !== "number") {
            return yield* error(projectId, run.runId, "completed", "projection-corrupt");
          }
          const finalStatus = noEligibleRows[0].found === 1 ? "no-eligible-task" : "completed";
          yield* commitStep(
            run,
            "completed",
            { schemaVersion: 1, status: finalStatus },
            bindings,
            nextState(run, { status: finalStatus }),
            project.state.updatedAt ?? activation.activatedAt,
          );
          if (admitSuccessor) {
            return yield* Effect.suspend(() => processSerialized(projectId));
          }
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
    projectLocks
      .withPermit(projectId, processSerialized(projectId))
      .pipe(
        Effect.mapError((cause) =>
          cause instanceof AgentControlRunOnceError
            ? cause
            : error(projectId, null, null, "persistence", cause),
        ),
      );

  const recover: AgentControlRunOnceControllerShape["recover"] = Effect.gen(function* () {
    yield* auditRecoveryAuthority().pipe(
      Effect.mapError((cause) =>
        cause instanceof AgentControlRunOnceError
          ? cause
          : error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
      ),
    );
    yield* recoverPublications();
    const rows = yield* sql<{ readonly projectId: unknown }>`
      SELECT project_id AS "projectId" FROM main.agent_control_run_once_states
      WHERE status = 'active'
      UNION
      SELECT project_id AS "projectId" FROM main.agent_control_project_states
      WHERE mode = 'run-once' AND paused_from_mode IS NULL
      ORDER BY "projectId"
    `.pipe(
      Effect.mapError((cause) =>
        error("run-once-recovery" as ProjectId, null, null, "persistence", cause),
      ),
    );
    const projectIds = yield* Effect.forEach(rows, (row) => {
      if (typeof row.projectId !== "string") {
        return error("run-once-recovery" as ProjectId, null, null, "projection-corrupt");
      }
      return Effect.succeed(row.projectId as ProjectId);
    });
    yield* Effect.forEach(projectIds, processProject, {
      concurrency: "unbounded",
      discard: true,
    });
  });

  const prepare: AgentControlRunOnceControllerShape["prepare"] = (activation) =>
    Effect.gen(function* () {
      const projectStream = yield* requireRunOnceMethod(
        projectEngine.subscribeDomainEvents,
        "AgentControlEngine.subscribeDomainEvents",
      );
      const taskStream = yield* taskEngine.subscribeDomainEvents;
      const recoveryReady = yield* Deferred.make<void>();
      const jobs = yield* FiberSet.make<void, never>();
      const reportProjectFailure = (failure: AgentControlRunOnceError) =>
        Effect.logError("Run-Once project listener failed", { failure });
      const scheduleProject = (projectId: ProjectId) =>
        FiberSet.run(
          jobs,
          Deferred.await(recoveryReady).pipe(
            Effect.andThen(activation.await),
            Effect.andThen(processProject(projectId)),
            Effect.catch(reportProjectFailure),
            Effect.catchCause((cause) =>
              Effect.logError("Run-Once project listener defect", { cause }),
            ),
          ),
          { startImmediately: true },
        ).pipe(Effect.asVoid);
      yield* superviseAgentControlRunOnceListener(
        projectStream,
        requireRunOnceMethod(
          projectEngine.subscribeDomainEvents,
          "AgentControlEngine.subscribeDomainEvents",
        ),
        (event) => scheduleProject(event.aggregateId),
        "project",
      );
      yield* superviseAgentControlRunOnceListener(
        taskStream,
        taskEngine.subscribeDomainEvents,
        (event) =>
          event.type === "agentControl.task.finalizedAfterVerification"
            ? scheduleProject(event.payload.projectId)
            : Effect.void,
        "task",
      );
      yield* Effect.yieldNow;
      yield* hooks.afterSubscriptionsBeforeRecovery;
      yield* recover;
      yield* Deferred.succeed(recoveryReady, undefined);
    });

  return AgentControlRunOnceController.of({
    recover,
    processProject,
    prepare,
    recoverPublicationConsumer,
    pullPublications,
    acknowledgePublication,
    subscribePublicationWakeups: PubSub.subscribe(publications).pipe(
      Effect.map(Stream.fromSubscription),
    ),
    subscribePublications: PubSub.subscribe(publications).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const AgentControlRunOnceControllerLive = Layer.effect(AgentControlRunOnceController, make);
