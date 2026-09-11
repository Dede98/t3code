import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlWorktreeRpcError,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlRunOnceController } from "../../runOnce/Services/AgentControlRunOnceController.ts";
import {
  admitRunOnceActivation,
  loadRunOnceModeAuthority,
  writeRunOnceStep,
  type RunOnceStateBinding,
} from "../../runOnce/authority.ts";
import { deriveAgentControlRunOnceId, deriveRunOnceCommandId } from "../../runOnce/identity.ts";
import { AgentControlTaskIntakeReactor } from "../../task/Services/AgentControlTaskIntakeReactor.ts";
import { canonicalJson } from "../../initialPlanning/eventEvidence.ts";
import { makeReactorStartupActivation } from "../../../reactorStartupActivation.ts";
import { AgentControlRunOnceError } from "../../runOnce/model.ts";
import { AgentControlArmedError } from "../model.ts";
import { make, makeAgentControlArmedWorkScheduler } from "./AgentControlArmedScheduler.ts";

const projectId = ProjectId.make("armed-scheduler-takeover");
const at = "2026-09-02T08:00:00.000Z";

interface ActivatedDispatchRow {
  readonly eventId: string;
  readonly sequence: number;
  readonly streamVersion: number;
  readonly commandId: string;
  readonly occurredAt: string;
  readonly dispatchId: string;
  readonly claimId: string;
  readonly markerId: string;
  readonly taskId: string;
  readonly githubIntakeSequence: number;
  readonly githubEventId: string;
  readonly githubEventSequence: number;
  readonly githubEventStreamVersion: number;
  readonly reconcileRevision: number;
  readonly sourceFingerprint: string;
}

const runOnceCalls = Ref.makeUnsafe(new Map<ProjectId, number>());
const runOnceEntered = Ref.makeUnsafe(new Map<ProjectId, Deferred.Deferred<void>>());
const runOnceHandlers = Ref.makeUnsafe(
  new Map<ProjectId, Effect.Effect<void, AgentControlRunOnceError>>(),
);
const resetRunOnce = (id: ProjectId, signal?: Deferred.Deferred<void>) =>
  Ref.update(runOnceCalls, (current) => {
    const next = new Map(current);
    next.delete(id);
    return next;
  }).pipe(
    Effect.andThen(
      Ref.update(runOnceEntered, (current) => {
        const next = new Map(current);
        if (signal === undefined) next.delete(id);
        else next.set(id, signal);
        return next;
      }),
    ),
  );
const getRunOnceCalls = (id: ProjectId) =>
  Ref.get(runOnceCalls).pipe(Effect.map((current) => current.get(id) ?? 0));
const setRunOnceHandler = (
  id: ProjectId,
  handler?: Effect.Effect<void, AgentControlRunOnceError>,
) =>
  Ref.update(runOnceHandlers, (current) => {
    const next = new Map(current);
    if (handler === undefined) next.delete(id);
    else next.set(id, handler);
    return next;
  });
const dependencies = Layer.mergeAll(
  AgentControlRuntimeLayerLive,
  Layer.succeed(
    AgentControlRunOnceController,
    AgentControlRunOnceController.of({
      recover: Effect.void,
      processProject: (id) =>
        Ref.update(runOnceCalls, (current) => {
          const next = new Map(current);
          next.set(id, (next.get(id) ?? 0) + 1);
          return next;
        }).pipe(
          Effect.andThen(Effect.all([Ref.get(runOnceEntered), Ref.get(runOnceHandlers)])),
          Effect.flatMap(([signals, handlers]) =>
            (signals.get(id) === undefined
              ? Effect.void
              : Deferred.succeed(signals.get(id)!, undefined).pipe(Effect.asVoid)
            ).pipe(Effect.andThen(handlers.get(id) ?? Effect.void)),
          ),
        ),
      prepare: () => Effect.void,
      subscribePublicationWakeups: Effect.succeed(Stream.empty),
      recoverPublicationConsumer: () => Effect.void,
      pullPublications: () => Effect.succeed([]),
      acknowledgePublication: () => Effect.void,
      subscribePublications: Effect.succeed(Stream.empty),
    }),
  ),
  Layer.succeed(
    AgentControlTaskIntakeReactor,
    AgentControlTaskIntakeReactor.of({
      start: () => Effect.void,
      getStatus: () => Effect.die("unused"),
      subscribeCompletions: Effect.succeed(Stream.empty),
    }),
  ),
).pipe(Layer.provideMerge(SqlitePersistenceMemory), Layer.provideMerge(NodeServices.layer));

const layer = it.layer(dependencies);

const addProject = (sql: SqlClient.SqlClient, id = projectId) => sql`
  INSERT INTO main.projection_projects (
    project_id, title, workspace_root, default_model_selection_json,
    scripts_json, created_at, updated_at, deleted_at
  ) VALUES (
    ${id}, 'Armed Scheduler', '/tmp/armed-scheduler', NULL, '[]', ${at}, ${at}, NULL
  )
`;

const seedSourceAndCandidate = Effect.fn("seedArmedSchedulerSource")(function* (
  sql: SqlClient.SqlClient,
  id = projectId,
  includeCandidate = true,
  candidateCount = includeCandidate ? 1 : 0,
) {
  const insertEvent = Effect.fn("insertArmedSchedulerEvent")(function* (input: {
    readonly eventId: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: Record<string, unknown>;
  }) {
    return (yield* sql<{ readonly sequence: number }>`
      INSERT INTO main.agent_control_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_authority,
        payload_json, metadata_json
      ) VALUES (
        ${input.eventId}, 'github-intake', ${id}, ${input.streamVersion},
        ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
        ${input.authority}, ${canonicalJson(input.payload as never)}, '{"schemaVersion":1}'
      ) RETURNING sequence
    `)[0]!.sequence;
  });
  yield* insertEvent({
    eventId: `${id}-github-config`,
    streamVersion: 1,
    eventType: "agentControl.github.config.set",
    commandId: `${id}-github-config-command`,
    authority: "human",
    payload: {
      projectId: id,
      repository: { repositoryNodeId: "armed-scheduler-repo", nameWithOwner: "owner/repo" },
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      configuredAt: at,
    },
  });
  const githubSequence = yield* insertEvent({
    eventId: `${id}-github-poll`,
    streamVersion: 2,
    eventType: "agentControl.github.poll.succeeded",
    commandId: `${id}-github-poll-command`,
    authority: "controller",
    payload: {
      projectId: id,
      repository: { repositoryNodeId: "armed-scheduler-repo", nameWithOwner: "owner/repo" },
      attemptedAt: at,
      completedAt: at,
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
      issues: Array.from({ length: candidateCount }, (_, index) => ({
        issueNodeId: `${id}-issue${index === 0 ? "" : `-${index + 1}`}`,
      })),
    },
  });
  const state = canonicalJson({
    schemaVersion: 1,
    projectId: id,
    config: {
      schemaVersion: 1,
      projectId,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      repository: { repositoryNodeId: "armed-scheduler-repo", nameWithOwner: "owner/repo" },
      revision: 2,
      sequence: githubSequence,
      updatedAt: at,
    },
    cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
    pollStatus: {
      status: "success",
      attemptedAt: at,
      completedAt: at,
      errorCode: null,
      issueCount: candidateCount,
    },
    revision: 2,
    sequence: githubSequence,
    updatedAt: at,
  } as never);
  yield* sql`
    INSERT INTO main.agent_control_github_intake_states (
      project_id, state_json, revision, last_event_sequence, updated_at
    ) VALUES (${id}, ${state}, 2, ${githubSequence}, ${at})
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence, revision, status, updated_at
    ) VALUES (${id}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
  `;
  if (!includeCandidate) return githubSequence;
  for (let index = 0; index < candidateCount; index++) {
    const ordinal = index + 1;
    yield* sql`
      INSERT INTO main.agent_control_task_states (
        task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
        status, source_gate, stage, source_updated_at, github_intake_sequence, state_json,
        created_at, updated_at, revision, last_event_sequence
      ) VALUES (
        ${`${id}-task${index === 0 ? "" : `-${ordinal}`}`}, ${id},
        'armed-scheduler-repo', ${`${id}-issue${index === 0 ? "" : `-${ordinal}`}`}, ${ordinal},
        ${`https://github.test/owner/repo/issues/${ordinal}`}, 'candidate', 'eligible', 'intake',
        ${at}, ${githubSequence}, '{}', ${at}, ${at}, 1, ${githubSequence + ordinal}
      )
    `;
  }
  return githubSequence;
});

const makeNoEligibleRunOnceHandler = (
  sql: SqlClient.SqlClient,
  engine: AgentControlEngine["Service"],
  id: ProjectId,
) =>
  Effect.gen(function* () {
    const rows = yield* sql<ActivatedDispatchRow>`
      SELECT event.event_id AS "eventId", event.sequence,
        event.stream_version AS "streamVersion", event.command_id AS "commandId",
        event.occurred_at AS "occurredAt", evidence.dispatch_id AS "dispatchId",
        evidence.claim_id AS "claimId", evidence.marker_id AS "markerId",
        evidence.selected_task_id AS "taskId",
        evidence.github_intake_sequence AS "githubIntakeSequence",
        evidence.github_event_id AS "githubEventId",
        evidence.github_event_sequence AS "githubEventSequence",
        evidence.github_event_stream_version AS "githubEventStreamVersion",
        evidence.reconcile_revision AS "reconcileRevision",
        evidence.source_fingerprint AS "sourceFingerprint"
      FROM main.agent_control_armed_dispatch_states state
      JOIN main.agent_control_armed_dispatch_evidence evidence
        ON evidence.dispatch_id = state.dispatch_id
      JOIN main.agent_control_events event ON event.event_id = state.activation_event_id
      WHERE state.project_id = ${id} AND state.status = 'activated'
    `;
    assert.lengthOf(rows, 1);
    const row = rows[0]!;
    assert.equal(typeof row.eventId, "string");
    assert.equal(typeof row.sequence, "number");
    assert.equal(typeof row.streamVersion, "number");
    assert.equal(typeof row.commandId, "string");
    assert.equal(typeof row.occurredAt, "string");
    assert.equal(typeof row.taskId, "string");
    const activationEvent = {
      sequence: row.sequence,
      streamVersion: row.streamVersion,
      eventId: EventId.make(row.eventId),
      type: "agentControl.project.mode.changed",
      aggregateKind: "project-controller",
      aggregateId: id,
      occurredAt: row.occurredAt,
      commandId: CommandId.make(row.commandId),
      causationEventId: null,
      correlationId: CommandId.make(row.commandId),
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: {
        projectId: id,
        previousMode: "armed",
        mode: "run-once",
        previousPausedFromMode: null,
        pausedFromMode: null,
        changedAt: row.occurredAt,
      },
    } as const satisfies AgentControlEvent;
    const runId = deriveAgentControlRunOnceId({
      projectId: id,
      activationEventId: activationEvent.eventId,
      activationEventSequence: activationEvent.sequence,
      activationEventStreamVersion: activationEvent.streamVersion,
      activationCommandId: activationEvent.commandId,
    });
    const modeAuthority = yield* loadRunOnceModeAuthority(sql, id, activationEvent);
    yield* admitRunOnceActivation(
      sql,
      {
        schemaVersion: 1,
        runId,
        projectId: id,
        activationEventId: activationEvent.eventId,
        activationEventSequence: activationEvent.sequence,
        activationEventStreamVersion: activationEvent.streamVersion,
        activationCommandId: activationEvent.commandId,
        originMode: "armed",
        armedDispatchId: row.dispatchId,
        armedClaimId: row.claimId,
        armedMarkerId: row.markerId,
        githubIntakeSequence: row.githubIntakeSequence,
        githubEventId: EventId.make(row.githubEventId),
        githubEventSequence: row.githubEventSequence,
        githubEventStreamVersion: row.githubEventStreamVersion,
        reconcileRevision: row.reconcileRevision,
        sourceFingerprint: row.sourceFingerprint,
        activatedAt: activationEvent.occurredAt,
      },
      modeAuthority,
    );

    const taskId = row.taskId;
    const state = (
      status: RunOnceStateBinding["status"],
      patch: Partial<RunOnceStateBinding>,
    ): RunOnceStateBinding => ({
      projectId: id,
      status,
      taskId: null,
      stageRunId: null,
      leaseId: null,
      worktreeReservationId: null,
      controlledThreadReservationId: null,
      terminalTaskEventId: null,
      activationProjectRevision: activationEvent.streamVersion,
      resetProjectRevision: null,
      ...patch,
    });
    const commit = (
      ordinal: number,
      step: Parameters<typeof writeRunOnceStep>[1]["step"],
      bindings: Parameters<typeof writeRunOnceStep>[1]["bindings"],
      next: RunOnceStateBinding,
    ) =>
      writeRunOnceStep(sql, {
        runId,
        projectId: id,
        ordinal,
        step,
        payload: { schemaVersion: 1, taskId, step },
        bindings,
        state: next,
        recordedAt: activationEvent.occurredAt,
      });
    yield* commit(1, "activation-admitted", {}, state("active", {}));
    // This scheduler-level test intentionally delegates Run-Once internals.
    // Make the dispatch-selected task non-candidate before the Run-Once
    // selection point, then complete the real no-eligible E/R/M reset path
    // through every installed Run-Once and M064 origin trigger. A separate
    // Run-Once integration test owns the terminal Task production seam.
    yield* sql`
      UPDATE main.agent_control_task_states
      SET source_gate = 'closed', revision = revision + 1,
        last_event_sequence = last_event_sequence + 100, updated_at = ${activationEvent.occurredAt}
      WHERE task_id = ${taskId} AND project_id = ${id} AND status = 'candidate'
    `;
    yield* commit(2, "no-eligible-task", {}, state("active", {}));

    const current = yield* engine.getProjectState({ projectId: id });
    const resetCommandId = deriveRunOnceCommandId(runId, 3, "mode-reset");
    const reset = yield* engine.dispatchSystem({
      commandId: resetCommandId,
      projectId: id,
      expectedRevision: current.revision,
      mode: "armed",
    });
    const resetRows = yield* sql<Record<string, unknown>>`
      SELECT event_id AS "eventId", sequence, stream_version AS "streamVersion",
        occurred_at AS "occurredAt"
      FROM main.agent_control_events WHERE command_id = ${resetCommandId}
    `;
    assert.lengthOf(resetRows, 1);
    const resetRow = resetRows[0]!;
    const resetEvent = {
      ...activationEvent,
      eventId: EventId.make(resetRow.eventId as string),
      sequence: resetRow.sequence as number,
      streamVersion: resetRow.streamVersion as number,
      occurredAt: resetRow.occurredAt as string,
      commandId: resetCommandId,
      correlationId: resetCommandId,
      payload: {
        projectId: id,
        previousMode: "run-once",
        mode: "armed",
        previousPausedFromMode: null,
        pausedFromMode: null,
        changedAt: resetRow.occurredAt as string,
      },
    } as const satisfies AgentControlEvent;
    const resetAuthority = yield* loadRunOnceModeAuthority(sql, id, resetEvent);
    yield* writeRunOnceStep(sql, {
      runId,
      projectId: id,
      ordinal: 3,
      step: "mode-reset",
      payload: { schemaVersion: 1, projectRevision: reset.state.revision },
      bindings: {
        modeEventId: resetEvent.eventId,
        modeEventSequence: resetEvent.sequence,
        modeEventStreamVersion: resetEvent.streamVersion,
        modeExpectedRevision: resetAuthority.expectedRevision,
        modeCommandFingerprint: resetAuthority.commandFingerprint,
        modeEventPayloadBytes: resetAuthority.eventPayloadBytes,
        modeEventMetadataBytes: resetAuthority.eventMetadataBytes,
      },
      state: state("active", {
        resetProjectRevision: reset.state.revision,
      }),
      recordedAt: resetEvent.occurredAt,
    });
    yield* commit(
      4,
      "completed",
      {},
      state("no-eligible-task", { resetProjectRevision: reset.state.revision }),
    );
  }).pipe(Effect.orDie);

it.effect("coalesces duplicate wakeups and bounds project execution", () =>
  Effect.gen(function* () {
    const stormProject = ProjectId.make("armed-work-scheduler-storm");
    const firstEntered = yield* Deferred.make<void>();
    const releaseFirst = yield* Deferred.make<void>();
    const secondCompleted = yield* Deferred.make<void>();
    const stormCalls = yield* Ref.make(0);
    const scheduleStorm = yield* makeAgentControlArmedWorkScheduler(
      () =>
        Ref.updateAndGet(stormCalls, (value) => value + 1).pipe(
          Effect.flatMap((call) =>
            call === 1
              ? Deferred.succeed(firstEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFirst)),
                )
              : Deferred.succeed(secondCompleted, undefined).pipe(Effect.asVoid),
          ),
        ),
      Effect.void,
      0,
    );
    yield* scheduleStorm(stormProject);
    yield* Deferred.await(firstEntered);
    yield* Effect.forEach(Array.from({ length: 500 }), () => scheduleStorm(stormProject), {
      concurrency: "unbounded",
      discard: true,
    });
    yield* Deferred.succeed(releaseFirst, undefined);
    yield* Deferred.await(secondCompleted);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(stormCalls), 2);

    const releaseBlocked = yield* Deferred.make<void>();
    const blockedEntered = yield* Deferred.make<void>();
    const healthyCompleted = yield* Deferred.make<void>();
    const scheduleProjects = yield* makeAgentControlArmedWorkScheduler(
      (id) =>
        id === ProjectId.make("armed-work-scheduler-blocked")
          ? Deferred.succeed(blockedEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseBlocked)),
            )
          : Deferred.succeed(healthyCompleted, undefined).pipe(Effect.asVoid),
      Effect.void,
      0,
    );
    yield* scheduleProjects(ProjectId.make("armed-work-scheduler-blocked"));
    yield* Deferred.await(blockedEntered);
    yield* scheduleProjects(ProjectId.make("armed-work-scheduler-healthy"));
    yield* Deferred.await(healthyCompleted);
    yield* Deferred.succeed(releaseBlocked, undefined);
  }),
);

it.effect("retries only transient failures with one coalesced TestClock timer", () =>
  Effect.gen(function* () {
    const transientProject = ProjectId.make("armed-work-scheduler-transient");
    const transientFirst = yield* Deferred.make<void>();
    const transientRecovered = yield* Deferred.make<void>();
    const transientCalls = yield* Ref.make(0);
    const scheduleTransient = yield* makeAgentControlArmedWorkScheduler(
      (id) =>
        Ref.updateAndGet(transientCalls, (value) => value + 1).pipe(
          Effect.flatMap((call) =>
            call === 1
              ? Deferred.succeed(transientFirst, undefined).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new AgentControlArmedError({ projectId: id, reason: "persistence" }),
                    ),
                  ),
                )
              : Deferred.succeed(transientRecovered, undefined).pipe(Effect.asVoid),
          ),
        ),
      Effect.void,
      2,
    );
    yield* scheduleTransient(transientProject);
    yield* Deferred.await(transientFirst);
    yield* Effect.forEach(Array.from({ length: 500 }), () => scheduleTransient(transientProject), {
      concurrency: "unbounded",
      discard: true,
    });
    yield* Effect.yieldNow;
    yield* TestClock.adjust(Duration.millis(24));
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(transientCalls), 1);
    yield* TestClock.adjust(Duration.millis(1));
    yield* Deferred.await(transientRecovered);
    assert.equal(yield* Ref.get(transientCalls), 2);

    const corruptProject = ProjectId.make("armed-work-scheduler-corrupt");
    const corruptFailed = yield* Deferred.make<void>();
    const corruptReported = yield* Deferred.make<AgentControlArmedError>();
    const corruptCalls = yield* Ref.make(0);
    const scheduleCorrupt = yield* makeAgentControlArmedWorkScheduler(
      (id) =>
        Ref.updateAndGet(corruptCalls, (value) => value + 1).pipe(
          Effect.andThen(Deferred.succeed(corruptFailed, undefined)),
          Effect.andThen(
            Effect.fail(
              new AgentControlArmedError({ projectId: id, reason: "authority-conflict" }),
            ),
          ),
        ),
      Effect.void,
      6,
      (failure) => Deferred.succeed(corruptReported, failure).pipe(Effect.asVoid),
    );
    yield* scheduleCorrupt(corruptProject);
    yield* Deferred.await(corruptFailed);
    const reported = yield* Deferred.await(corruptReported);
    assert.equal(reported.reason, "authority-conflict");
    assert.equal(reported.projectId, corruptProject);
    yield* Effect.yieldNow;
    yield* TestClock.adjust(Duration.hours(1));
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(corruptCalls), 1);
  }),
);

it.effect("interrupts active workers when the scheduler scope closes", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const schedule = yield* makeAgentControlArmedWorkScheduler(
          () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() =>
                Deferred.succeed(interrupted, undefined).pipe(Effect.asVoid),
              ),
            ),
          Effect.void,
          0,
        );
        yield* schedule(ProjectId.make("armed-work-scheduler-shutdown"));
        yield* Deferred.await(entered);
      }),
    );
    yield* Deferred.await(interrupted);
  }),
);

layer("AgentControlArmedScheduler", (it) => {
  it.effect("releases the shared fence after activation and honors immediate Human takeover", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      yield* resetRunOnce(projectId);
      yield* addProject(sql);
      yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-observe"),
        projectId,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-arm"),
        projectId,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql);
      const beforeRunOnce = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const scheduler = yield* make({
        hooks: {
          beforeRunOnce: () =>
            Deferred.succeed(beforeRunOnce, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
            ),
        },
      });
      const processing = yield* Effect.forkScoped(scheduler.processProject(projectId));
      yield* Deferred.await(beforeRunOnce);
      const activated = yield* engine.getProjectState({ projectId });
      assert.equal(activated.mode, "run-once");

      const takeover = yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-takeover"),
        projectId,
        expectedRevision: activated.revision,
        mode: "observe",
      });
      assert.equal(takeover.state.mode, "observe");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(processing);

      assert.equal(yield* getRunOnceCalls(projectId), 0);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status,
            (SELECT count(*) FROM main.agent_control_run_once_activations) AS activations,
            (SELECT count(*) FROM main.agent_control_run_once_states) AS runs
          FROM main.agent_control_armed_dispatch_states
        `,
        [{ status: "superseded", activations: 0, runs: 0 }],
      );

      const rearmed = yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-rearm-after-takeover"),
        projectId,
        expectedRevision: takeover.state.revision,
        mode: "armed",
      });
      assert.equal(rearmed.state.mode, "armed");
      const modeEventWritten = yield* Deferred.make<void>();
      const crashBoundary = yield* Deferred.make<void>();
      const crashingScheduler = yield* make({
        hooks: {
          afterModeEvent: () =>
            Deferred.succeed(modeEventWritten, undefined).pipe(
              Effect.andThen(Deferred.await(crashBoundary)),
            ),
        },
      });
      const crashing = yield* Effect.forkScoped(crashingScheduler.processProject(projectId));
      yield* Deferred.await(modeEventWritten);
      assert.deepStrictEqual(
        yield* sql`
          SELECT dispatch.status, project.mode
          FROM main.agent_control_armed_dispatch_states dispatch
          JOIN main.agent_control_project_states project ON project.project_id = dispatch.project_id
          WHERE dispatch.status = 'claimed'
        `,
        [{ status: "claimed", mode: "run-once" }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT event.aggregate_kind AS "aggregateKind", event.event_type AS "eventType",
            event.actor_authority AS authority, receipt.status,
            receipt.event_created AS "eventCreated"
          FROM main.agent_control_events event
          JOIN main.agent_control_command_receipts receipt ON receipt.command_id = event.command_id
          WHERE event.stream_id = ${projectId} AND event.actor_authority = 'system'
          ORDER BY event.sequence DESC LIMIT 1
        `,
        [
          {
            aggregateKind: "project-controller",
            eventType: "agentControl.project.mode.changed",
            authority: "system",
            status: "accepted",
            eventCreated: 1,
          },
        ],
      );
      yield* Fiber.interrupt(crashing);

      const recoveringScheduler = yield* make();
      yield* recoveringScheduler.processProject(projectId);
      assert.equal(yield* getRunOnceCalls(projectId), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${projectId} ORDER BY updated_at, dispatch_id
        `,
        [{ status: "activated" }, { status: "superseded" }],
      );
    }),
  );

  it.effect("catches up durable work after a lost intake wakeup before readiness", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const id = ProjectId.make("armed-scheduler-lost-wakeup");
      const entered = yield* Deferred.make<void>();
      yield* resetRunOnce(id, entered);
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-lost-observe"),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make("armed-scheduler-lost-arm"),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      const githubSequence = yield* seedSourceAndCandidate(sql, id, false);
      const scheduler = yield* make();
      const activation = yield* makeReactorStartupActivation;
      yield* scheduler.prepare(activation);
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count FROM main.agent_control_armed_no_candidate_evidence
          WHERE project_id = ${id}
        `,
        [{ count: 1 }],
      );

      yield* sql`
        INSERT INTO main.agent_control_task_states (
          task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
          status, source_gate, stage, source_updated_at, github_intake_sequence, state_json,
          created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          ${`${id}-task`}, ${id}, 'armed-scheduler-repo', ${`${id}-issue`}, 1,
          'https://github.test/owner/repo/issues/1', 'candidate', 'eligible', 'intake',
          ${at}, ${githubSequence}, '{}', ${at}, ${at}, 1, ${githubSequence + 1}
        )
      `;
      yield* activation.open;
      yield* Deferred.await(entered);
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id}
        `,
        [{ status: "activated" }],
      );
      yield* resetRunOnce(id);
    }),
  );

  it.effect("rotates an expired same-owner claim and retries without another wakeup", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const id = ProjectId.make("armed-scheduler-expired-same-owner");
      const entered = yield* Deferred.make<void>();
      const firstClaimExpired = yield* Deferred.make<void>();
      const expireFirst = yield* Ref.make(true);
      yield* resetRunOnce(id, entered);
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe-command`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm-command`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);
      const scheduler = yield* make({
        claimDurationMs: 1,
        hooks: {
          afterClaim: () =>
            Ref.getAndSet(expireFirst, false).pipe(
              Effect.flatMap((shouldExpire) =>
                shouldExpire
                  ? TestClock.adjust(Duration.millis(1)).pipe(
                      Effect.andThen(Deferred.succeed(firstClaimExpired, undefined)),
                      Effect.asVoid,
                    )
                  : Effect.void,
              ),
            ),
        },
      });
      const schedule = yield* makeAgentControlArmedWorkScheduler(
        scheduler.processProject,
        Effect.void,
        2,
      );
      yield* schedule(id);
      yield* Deferred.await(firstClaimExpired);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.millis(25));
      yield* Deferred.await(entered);
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, fence_token AS "fenceToken"
          FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id} ORDER BY fence_token
        `,
        [
          { status: "superseded", fenceToken: 1 },
          { status: "activated", fenceToken: 2 },
        ],
      );
      yield* resetRunOnce(id);
    }),
  );

  it.effect("recovers a foreign pre-mode claim after restart without another wakeup", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const id = ProjectId.make("armed-scheduler-crash-after-claim");
      const claimed = yield* Deferred.make<void>();
      const entered = yield* Deferred.make<void>();
      yield* resetRunOnce(id, entered);
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe-command`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm-command`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);

      const first = yield* make({
        claimDurationMs: 25,
        hooks: {
          afterClaim: () => Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Effect.never)),
        },
      });
      const interrupted = yield* Effect.forkScoped(first.processProject(id));
      yield* Deferred.await(claimed);
      assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "armed");
      yield* Fiber.interrupt(interrupted);

      const restarted = yield* make({ claimDurationMs: 25 });
      const schedule = yield* makeAgentControlArmedWorkScheduler(
        restarted.processProject,
        Effect.void,
        2,
      );
      yield* schedule(id);
      yield* Effect.yieldNow;
      assert.equal(yield* getRunOnceCalls(id), 0);
      yield* TestClock.adjust(Duration.millis(25));
      yield* Deferred.await(entered);
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, fence_token AS "fenceToken"
          FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id} ORDER BY fence_token
        `,
        [
          { status: "superseded", fenceToken: 1 },
          { status: "activated", fenceToken: 2 },
        ],
      );
      yield* resetRunOnce(id);
    }),
  );

  it.effect("recovers an activated dispatch after restart before Run-Once admission", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const id = ProjectId.make("armed-scheduler-crash-after-activation");
      const activated = yield* Deferred.make<void>();
      yield* resetRunOnce(id);
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe-command`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm-command`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);

      const first = yield* make({
        hooks: {
          afterActivation: () =>
            Deferred.succeed(activated, undefined).pipe(Effect.andThen(Effect.never)),
        },
      });
      const interrupted = yield* Effect.forkScoped(first.processProject(id));
      yield* Deferred.await(activated);
      assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "run-once");
      assert.equal(yield* getRunOnceCalls(id), 0);
      yield* Fiber.interrupt(interrupted);

      const restarted = yield* make();
      yield* restarted.processProject(id);
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, activation_event_id IS NOT NULL AS "hasActivation"
          FROM main.agent_control_armed_dispatch_states WHERE project_id = ${id}
        `,
        [{ status: "activated", hasActivation: 1 }],
      );
    }),
  );

  it.effect(
    "keeps Human pause and manual takeover authoritative and resumes paused Armed work",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;

        for (const takeover of ["observe", "manual", "paused"] as const) {
          const id = ProjectId.make(`armed-scheduler-${takeover}-before-claim`);
          yield* resetRunOnce(id);
          yield* addProject(sql, id);
          yield* engine.dispatchHuman({
            commandId: CommandId.make(`${id}-enter-observe-command`),
            projectId: id,
            expectedRevision: 0,
            mode: "observe",
          });
          yield* engine.dispatchHuman({
            commandId: CommandId.make(`${id}-arm-command`),
            projectId: id,
            expectedRevision: 1,
            mode: "armed",
          });
          yield* seedSourceAndCandidate(sql, id);
          const taken = yield* engine.dispatchHuman({
            commandId: CommandId.make(`${id}-${takeover}-command`),
            projectId: id,
            expectedRevision: 2,
            mode: takeover,
          });
          assert.equal(taken.state.mode, takeover);
          assert.equal(taken.state.pausedFromMode, takeover === "paused" ? "armed" : null);

          const scheduler = yield* make();
          yield* scheduler.processProject(id);
          assert.equal(yield* getRunOnceCalls(id), 0);
          assert.deepStrictEqual(
            yield* sql`
            SELECT count(*) AS count FROM main.agent_control_armed_dispatch_states
            WHERE project_id = ${id}
          `,
            [{ count: 0 }],
          );

          if (takeover === "paused") {
            const resumed = yield* engine.dispatchHuman({
              commandId: CommandId.make(`${id}-resume-command`),
              projectId: id,
              expectedRevision: taken.state.revision,
              mode: "armed",
            });
            assert.equal(resumed.state.mode, "armed");
            assert.equal(resumed.state.pausedFromMode, null);
            yield* scheduler.processProject(id);
            assert.equal(yield* getRunOnceCalls(id), 1);
            assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "run-once");
          }
        }
      }),
  );

  it.effect(
    "fences a queued Human takeover after claim and honors its fresh retry before Run-Once",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;
        const id = ProjectId.make("armed-scheduler-takeover-after-claim");
        yield* resetRunOnce(id);
        yield* addProject(sql, id);
        yield* engine.dispatchHuman({
          commandId: CommandId.make(`${id}-observe-command`),
          projectId: id,
          expectedRevision: 0,
          mode: "observe",
        });
        yield* engine.dispatchHuman({
          commandId: CommandId.make(`${id}-arm-command`),
          projectId: id,
          expectedRevision: 1,
          mode: "armed",
        });
        yield* seedSourceAndCandidate(sql, id);
        const claimed = yield* Deferred.make<void>();
        const releaseClaim = yield* Deferred.make<void>();
        const beforeRunOnce = yield* Deferred.make<void>();
        const releaseRunOnce = yield* Deferred.make<void>();
        const scheduler = yield* make({
          hooks: {
            afterClaim: () =>
              Deferred.succeed(claimed, undefined).pipe(
                Effect.andThen(Deferred.await(releaseClaim)),
              ),
            beforeRunOnce: () =>
              Deferred.succeed(beforeRunOnce, undefined).pipe(
                Effect.andThen(Deferred.await(releaseRunOnce)),
              ),
          },
        });
        const processing = yield* Effect.forkScoped(scheduler.processProject(id));
        yield* Deferred.await(claimed);
        const staleTakeover = yield* Effect.forkScoped(
          Effect.exit(
            engine.dispatchHuman({
              commandId: CommandId.make(`${id}-stale-manual-command`),
              projectId: id,
              expectedRevision: 2,
              mode: "manual",
            }),
          ),
        );
        yield* Effect.yieldNow;
        assert.equal(staleTakeover.pollUnsafe(), undefined);
        yield* Deferred.succeed(releaseClaim, undefined);
        yield* Deferred.await(beforeRunOnce);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(staleTakeover)));
        const activated = yield* engine.getProjectState({ projectId: id });
        assert.equal(activated.mode, "run-once");
        const taken = yield* engine.dispatchHuman({
          commandId: CommandId.make(`${id}-manual-command`),
          projectId: id,
          expectedRevision: activated.revision,
          mode: "manual",
        });
        yield* Deferred.succeed(releaseRunOnce, undefined);
        yield* Fiber.join(processing);
        assert.equal(taken.state.mode, "manual");
        assert.equal(yield* getRunOnceCalls(id), 0);
        assert.deepStrictEqual(
          yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id}
        `,
          [{ status: "superseded" }],
        );
      }),
  );

  it.effect("never rearms a Human takeover that lands while Run-Once is active", () => {
    const id = ProjectId.make("armed-scheduler-takeover-during-run-once");
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* resetRunOnce(id);
      yield* setRunOnceHandler(
        id,
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      );
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe-command`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm-command`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);
      const scheduler = yield* make();
      const processing = yield* Effect.forkScoped(scheduler.processProject(id));
      yield* Deferred.await(entered);
      const active = yield* engine.getProjectState({ projectId: id });
      assert.equal(active.mode, "run-once");
      const taken = yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-manual-command`),
        projectId: id,
        expectedRevision: active.revision,
        mode: "manual",
      });
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(processing);
      assert.equal(taken.state.mode, "manual");
      assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "manual");
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id}
        `,
        [{ status: "superseded" }],
      );
    }).pipe(Effect.ensuring(setRunOnceHandler(id)));
  });

  it.effect("recovers after Run-Once rearmed before Armed completion and does not spin", () => {
    const id = ProjectId.make("armed-scheduler-rearm-before-finish");
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      yield* resetRunOnce(id);
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe-command`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm-command`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);
      yield* setRunOnceHandler(id, makeNoEligibleRunOnceHandler(sql, engine, id));
      const crashing = yield* make({
        hooks: { afterRunOnce: () => Effect.die(new Error("crash after rearm")) },
      });
      assert.isTrue(Exit.isFailure(yield* Effect.exit(crashing.processProject(id))));
      assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "armed");
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id}
        `,
        [{ status: "activated" }],
      );
      const restarted = yield* make();
      yield* restarted.processProject(id);
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${id}
        `,
        [{ status: "completed" }],
      );
      yield* Effect.forEach(Array.from({ length: 100 }), () => restarted.processProject(id), {
        discard: true,
      });
      assert.equal(yield* getRunOnceCalls(id), 1);
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count
          FROM main.agent_control_armed_no_candidate_markers marker
          JOIN main.agent_control_armed_no_candidate_evidence evidence
            ON evidence.evidence_id = marker.evidence_id
          WHERE evidence.project_id = ${id}
        `,
        [{ count: 1 }],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
    }).pipe(Effect.ensuring(setRunOnceHandler(id)));
  });
});

layer("Armed recovery failure boundary", (it) => {
  it.effect("keeps a recovered project blocker isolated after Armed workers activate", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const id = ProjectId.make("armed-recovery-local-blocker");
      yield* addProject(sql, id);
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-observe`),
        projectId: id,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* engine.dispatchHuman({
        commandId: CommandId.make(`${id}-arm`),
        projectId: id,
        expectedRevision: 1,
        mode: "armed",
      });
      yield* seedSourceAndCandidate(sql, id);
      const blocker = new AgentControlRunOnceError({
        projectId: id,
        runId: null,
        step: "worktree-ready",
        reason: "downstream-rejected",
        cause: new AgentControlWorktreeRpcError({
          projectId: id,
          taskId: null,
          reservationId: null,
          operation: "reserve",
          code: "default-remote-ref-unavailable",
        }),
      });
      yield* setRunOnceHandler(id, Effect.fail(blocker));
      yield* Effect.addFinalizer(() => setRunOnceHandler(id));
      const wakeups = yield* PubSub.unbounded<ProjectId>();
      const intake = yield* AgentControlTaskIntakeReactor;
      const scheduler = yield* make().pipe(
        Effect.provideService(AgentControlTaskIntakeReactor, {
          ...intake,
          subscribeCompletions: PubSub.subscribe(wakeups).pipe(Effect.map(Stream.fromSubscription)),
        }),
      );
      const activation = yield* makeReactorStartupActivation;
      yield* scheduler.prepare(activation);
      assert.equal(yield* getRunOnceCalls(id), 1);
      const entered = yield* Deferred.make<void>();
      yield* resetRunOnce(id, entered);
      yield* activation.open;
      yield* Deferred.await(entered);
      yield* resetRunOnce(id);
      const wakeupEntered = yield* Deferred.make<void>();
      yield* resetRunOnce(id, wakeupEntered);
      yield* PubSub.publish(wakeups, id);
      yield* Deferred.await(wakeupEntered);
      yield* TestClock.adjust(Duration.hours(1));
      assert.equal(yield* getRunOnceCalls(id), 1);
      const failure = yield* Effect.forkScoped(scheduler.awaitFailure);
      yield* Effect.yieldNow;
      assert.isUndefined(failure.pollUnsafe());
      assert.deepStrictEqual(
        yield* sql`SELECT status FROM agent_control_armed_dispatch_states WHERE project_id=${id}`,
        [{ status: "activated" }],
      );
      assert.equal((yield* engine.getProjectState({ projectId: id })).mode, "run-once");
    }),
  );

  it.effect.each(["persistence", "authority-conflict", "projection-corrupt", "defect"] as const)(
    "keeps %s fatal during Armed recovery",
    (reason) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;
        const id = ProjectId.make(`armed-recovery-fatal-${reason}`);
        yield* addProject(sql, id);
        yield* engine.dispatchHuman({
          commandId: CommandId.make(`${id}-observe`),
          projectId: id,
          expectedRevision: 0,
          mode: "observe",
        });
        yield* engine.dispatchHuman({
          commandId: CommandId.make(`${id}-arm`),
          projectId: id,
          expectedRevision: 1,
          mode: "armed",
        });
        yield* seedSourceAndCandidate(sql, id);
        const defect = new Error("unexpected Armed downstream defect");
        yield* setRunOnceHandler(
          id,
          reason === "defect"
            ? Effect.die(defect)
            : Effect.fail(
                new AgentControlRunOnceError({
                  projectId: id,
                  runId: null,
                  step: "worktree-ready",
                  reason,
                }),
              ),
        );
        yield* Effect.addFinalizer(() => setRunOnceHandler(id));
        const scheduler = yield* make();
        const activation = yield* makeReactorStartupActivation;
        const result = yield* Effect.exit(scheduler.prepare(activation));
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) {
          const failures = result.cause.reasons;
          assert.lengthOf(failures, 1);
          if (reason === "defect") {
            assert.equal(failures[0]?._tag, "Die");
            if (failures[0]?._tag === "Die") assert.equal(failures[0].defect, defect);
          } else {
            const failure = yield* scheduler.recover.pipe(Effect.flip);
            assert.equal(
              failure.reason,
              reason === "persistence" ? "persistence" : "authority-conflict",
            );
            assert.equal(failure.projectId, id);
          }
        }
      }),
  );
});
