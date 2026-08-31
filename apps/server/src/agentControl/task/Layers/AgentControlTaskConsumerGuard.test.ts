import {
  AgentControlRunOnceId,
  type AgentControlGithubIssueSnapshot,
  AgentControlTaskId,
  CommandId,
  EventId,
  type AgentControlTaskState,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlTaskConsumerGuard } from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { loadAuthoritativeTaskProjectHistory } from "../authoritative.ts";
import { deriveAgentControlTaskId } from "../identity.ts";
import { layer } from "./AgentControlTaskConsumerGuard.ts";
import { canonicalJson, type JsonValue } from "../../initialPlanning/eventEvidence.ts";
import {
  admitRunOnceActivation,
  fingerprintRunOnceModeCommand,
  writeRunOnceStep,
} from "../../runOnce/authority.ts";
import { deriveAgentControlRunOnceId } from "../../runOnce/identity.ts";
import { fingerprintAgentControlRunOnceSource } from "../../runOnce/source.ts";

const projectId = ProjectId.make("task-consumer-guard");
const at = "2026-07-23T00:00:00.000Z";
const taskId = AgentControlTaskId.make(
  "github-3f171876f35abbfbf47f165f5d4efc6a9175d46950e4aac1460a64135dbfde12",
);
const task = (sequence: number): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId,
  source: {
    projectId,
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    issueNumber: 1,
    issueUrl: "https://example.test/issues/1",
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: at,
  githubIntakeSequence: sequence,
  sourceSnapshot: {
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    number: 1,
    url: "https://example.test/issues/1",
    state: "open",
    title: "Untrusted title",
    body: null,
    contentTrust: "untrusted-external",
    updatedAt: at,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt: at,
  updatedAt: at,
  revision: 1,
  sequence: 1,
});
const issue: AgentControlGithubIssueSnapshot = {
  ...task(5).sourceSnapshot,
  timelineEvents: [],
} as const;

const taskEvent = (state: AgentControlTaskState) => {
  const commandId = CommandId.make(`command-${state.taskId}`);
  return {
    sequence: state.sequence,
    streamVersion: state.revision,
    eventId: EventId.make(`event-${state.taskId}`),
    type: "agentControl.task.created" as const,
    aggregateKind: "task" as const,
    aggregateId: state.taskId,
    occurredAt: state.createdAt,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller" as const,
    metadata: { schemaVersion: 1 as const },
    payload: {
      taskId: state.taskId,
      source: state.source,
      status: "candidate" as const,
      sourceGate: state.sourceGate,
      stage: "intake" as const,
      sourceUpdatedAt: state.sourceUpdatedAt,
      githubIntakeSequence: state.githubIntakeSequence,
      sourceSnapshot: state.sourceSnapshot,
      createdAt: state.createdAt,
    },
  };
};

const insertAuthorityEvent = Effect.fn("insertTaskGuardRunOnceEvent")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly eventId: string;
    readonly aggregateKind: string;
    readonly streamId: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: JsonValue;
  },
) {
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO main.agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      ${input.eventId}, ${input.aggregateKind}, ${input.streamId}, ${input.streamVersion},
      ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
      ${input.authority}, ${canonicalJson(input.payload)}, '{"schemaVersion":1}'
    ) RETURNING sequence
  `;
  return rows[0]!.sequence;
});

const seedSelectedRunOnceTask = Effect.fn("seedSelectedRunOnceTask")(function* (
  sql: SqlClient.SqlClient,
) {
  yield* insertAuthorityEvent(sql, {
    eventId: "task-guard-observe-event",
    aggregateKind: "project-controller",
    streamId: projectId,
    streamVersion: 1,
    eventType: "agentControl.project.mode.changed",
    commandId: "task-guard-observe-command",
    authority: "human",
    payload: {
      projectId,
      previousMode: "manual",
      mode: "observe",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  yield* insertAuthorityEvent(sql, {
    eventId: "task-guard-github-config-event",
    aggregateKind: "github-intake",
    streamId: projectId,
    streamVersion: 1,
    eventType: "agentControl.github.config.set",
    commandId: "task-guard-github-config-command",
    authority: "human",
    payload: {
      projectId,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      repository: { repositoryNodeId: "repository-node", nameWithOwner: "owner/repo" },
      configuredAt: at,
    },
  });
  const githubSequence = yield* insertAuthorityEvent(sql, {
    eventId: "task-guard-github-success-event",
    aggregateKind: "github-intake",
    streamId: projectId,
    streamVersion: 2,
    eventType: "agentControl.github.poll.succeeded",
    commandId: "task-guard-github-success-command",
    authority: "controller",
    payload: {
      projectId,
      repository: { repositoryNodeId: "repository-node", nameWithOwner: "owner/repo" },
      attemptedAt: at,
      completedAt: at,
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
      issues: [issue],
    },
  });
  const selectedTask = { ...task(githubSequence), sequence: githubSequence + 1 };
  const created = taskEvent(selectedTask);
  yield* insertAuthorityEvent(sql, {
    eventId: created.eventId,
    aggregateKind: created.aggregateKind,
    streamId: selectedTask.taskId,
    streamVersion: created.streamVersion,
    eventType: created.type,
    commandId: created.commandId,
    authority: created.authority,
    payload: created.payload as unknown as JsonValue,
  });
  const activationCommandId = CommandId.make("task-guard-run-once-command");
  const activationEventId = EventId.make("task-guard-run-once-event");
  const activationPayload = {
    projectId,
    previousMode: "observe",
    mode: "run-once",
    previousPausedFromMode: null,
    pausedFromMode: null,
    changedAt: at,
  } as const;
  const activationSequence = yield* insertAuthorityEvent(sql, {
    eventId: activationEventId,
    aggregateKind: "project-controller",
    streamId: projectId,
    streamVersion: 2,
    eventType: "agentControl.project.mode.changed",
    commandId: activationCommandId,
    authority: "human",
    payload: activationPayload,
  });
  const activationCommandFingerprint = fingerprintRunOnceModeCommand({
    commandId: activationCommandId,
    projectId,
    expectedRevision: 1,
    mode: "run-once",
  });
  yield* sql`
    INSERT INTO main.agent_control_command_receipts (
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created, accepted_at, error_code
    ) VALUES (
      ${activationCommandId}, ${activationCommandFingerprint}, 'human', 'project-controller',
      ${projectId}, 'accepted', ${activationSequence}, 2, 1, ${at}, NULL
    )
  `;
  const stateJson = new TextEncoder().encode(canonicalJson(selectedTask as unknown as JsonValue));
  yield* sql`
    INSERT INTO main.agent_control_task_states (
      task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
      status, source_gate, stage, source_updated_at, github_intake_sequence, state_json,
      created_at, updated_at, revision, last_event_sequence
    ) VALUES (
      ${selectedTask.taskId}, ${projectId}, 'repository-node', 'issue-node', 1,
      'https://example.test/issues/1', 'candidate', 'eligible', 'intake', ${at},
      ${githubSequence}, ${stateJson}, ${at}, ${at}, 1, ${selectedTask.sequence}
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'run-once', NULL, 2, ${activationSequence}, ${at})
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence, revision, status, updated_at
    ) VALUES (${projectId}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
  `;
  const runId = deriveAgentControlRunOnceId({
    projectId,
    activationEventId,
    activationEventSequence: activationSequence,
    activationEventStreamVersion: 2,
    activationCommandId,
  });
  const activation = {
    schemaVersion: 1,
    runId,
    projectId,
    activationEventId,
    activationEventSequence: activationSequence,
    activationEventStreamVersion: 2,
    activationCommandId,
    githubIntakeSequence: githubSequence,
    githubEventId: EventId.make("task-guard-github-success-event"),
    githubEventSequence: githubSequence,
    githubEventStreamVersion: 2,
    reconcileRevision: 1,
    sourceFingerprint: fingerprintAgentControlRunOnceSource({
      schemaVersion: 1,
      projectId,
      githubIntakeSequence: githubSequence,
      githubProjectionRevision: 2,
      githubConfigRevision: 2,
      repositoryNodeId: "repository-node",
      pollStatus: "success",
      expectedIssueCount: 1,
    }),
    activatedAt: at,
  } as const;
  yield* admitRunOnceActivation(sql, activation, {
    expectedRevision: 1,
    commandFingerprint: activationCommandFingerprint,
    eventPayloadBytes: new TextEncoder().encode(canonicalJson(activationPayload)),
    eventMetadataBytes: new TextEncoder().encode('{"schemaVersion":1}'),
  });
  const initialState = {
    projectId,
    status: "active" as const,
    taskId: null,
    stageRunId: null,
    leaseId: null,
    worktreeReservationId: null,
    controlledThreadReservationId: null,
    terminalTaskEventId: null,
    activationProjectRevision: 2,
    resetProjectRevision: null,
  };
  yield* writeRunOnceStep(sql, {
    runId,
    projectId,
    ordinal: 1,
    step: "activation-admitted",
    payload: { schemaVersion: 1, activation: activation as unknown as JsonValue },
    bindings: {},
    state: initialState,
    recordedAt: at,
  });
  yield* writeRunOnceStep(sql, {
    runId,
    projectId,
    ordinal: 2,
    step: "task-selected",
    payload: { schemaVersion: 1, taskId: selectedTask.taskId },
    bindings: { taskId: selectedTask.taskId },
    state: { ...initialState, taskId: selectedTask.taskId },
    recordedAt: at,
  });
  return { runId, selectedTask, githubSequence } as const;
});

const makeGuard = (input?: {
  readonly available?: boolean;
  readonly mode?: "manual" | "observe" | "paused";
  readonly sourceSequence?: number | null;
  readonly watermarkStatus?: "reconciling" | "completed" | "recovery-required" | null;
  readonly targetSequence?: number;
  readonly lastCompletedSequence?: number;
  readonly watermarkRevision?: number;
  readonly sourceProjectionRevision?: number;
  readonly sourceConfigRevision?: number;
  readonly tasks?: ReadonlyArray<AgentControlTaskState | "corrupt">;
  readonly events?: ReadonlyArray<ReturnType<typeof taskEvent>>;
  readonly issues?: ReadonlyArray<AgentControlGithubIssueSnapshot>;
  readonly getCorrupt?: boolean;
  readonly getSqlError?: boolean;
}) =>
  AgentControlTaskConsumerGuard.pipe(
    Effect.provide(layer),
    Effect.provideService(AgentControlProjectAvailability, {
      ensureAvailable: () =>
        input?.available === false
          ? Effect.fail({
              _tag: "AgentControlProjectUnavailableError",
              projectId,
              reason: "deleted",
            } as never)
          : Effect.void,
    }),
    Effect.provideService(AgentControlProjectStateRepository, {
      get: () =>
        Effect.succeed(
          Option.some({
            schemaVersion: 1,
            projectId,
            mode: input?.mode ?? "observe",
            pausedFromMode: null,
            revision: 1,
            sequence: 1,
            updatedAt: at,
          }),
        ),
      save: () => Effect.die("unused"),
      listPersisted: Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
    Effect.provideService(AgentControlGithubStateRepository, {
      get: () => Effect.die("unused"),
      save: () => Effect.die("unused"),
      replaceIssues: () => Effect.die("unused"),
      listIssues: () => Effect.die("unused"),
      getCompletedSnapshot: () => {
        const sourceSequence = input?.sourceSequence === undefined ? 5 : input.sourceSequence;
        const issues = input?.issues ?? [issue];
        return Effect.succeed(
          sourceSequence === null
            ? Option.none()
            : Option.some({
                sourcePrecondition: {
                  schemaVersion: 1,
                  projectId,
                  githubIntakeSequence: sourceSequence,
                  githubProjectionRevision: input?.sourceProjectionRevision ?? 3,
                  githubConfigRevision: input?.sourceConfigRevision ?? 2,
                  repositoryNodeId: "repository-node",
                  pollStatus: "success",
                  expectedIssueCount: issues.length,
                },
                issues,
              }),
        );
      },
      matchesCompletedSnapshot: () => Effect.die("unused"),
      deleteProject: () => Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
    Effect.provideService(AgentControlTaskReconcileStateRepository, {
      get: () => {
        const status = input?.watermarkStatus === undefined ? "completed" : input.watermarkStatus;
        return Effect.succeed(
          status === null
            ? Option.none()
            : Option.some({
                schemaVersion: 1,
                projectId,
                targetSequence: input?.targetSequence ?? 5,
                lastCompletedSequence: input?.lastCompletedSequence ?? 5,
                revision: input?.watermarkRevision ?? 1,
                status,
                updatedAt: at,
              }),
        );
      },
      begin: () => Effect.die("unused"),
      markRecoveryRequired: () => Effect.die("unused"),
      complete: () => Effect.die("unused"),
    }),
    Effect.provideService(AgentControlTaskStateRepository, {
      get: (taskId) => {
        if (input?.getSqlError === true) {
          return Effect.fail({ _tag: "AgentControlPersistenceSqlError" } as never);
        }
        if (input?.getCorrupt === true) {
          return Effect.fail({ _tag: "AgentControlPersistenceDecodeError" } as never);
        }
        const entry = (input?.tasks ?? [task(5)]).find(
          (candidate) => candidate !== "corrupt" && candidate.taskId === taskId,
        );
        return Effect.succeed(
          entry === undefined || entry === "corrupt" ? Option.none() : Option.some(entry),
        );
      },
      save: () => Effect.die("unused"),
      listProject: () =>
        Effect.succeed(
          (input?.getCorrupt === true ? (["corrupt"] as const) : (input?.tasks ?? [task(5)])).map(
            (entry) =>
              entry === "corrupt"
                ? { _tag: "Corrupt" as const, taskId: null, projectId }
                : { _tag: "Valid" as const, state: entry },
          ),
        ),
      listAll: Effect.die("unused"),
      findByIdentity: () => Effect.die("unused"),
      findBySourceNumber: () => Effect.die("unused"),
      deleteAll: Effect.die("unused"),
    }),
    Effect.provideService(AgentControlTaskEventStore, {
      append: () => Effect.die("unused"),
      readStream: () => Effect.die("unused"),
      readGlobal: (after = 0, limit = 500) => {
        if (input?.getSqlError === true) {
          return Effect.fail({ _tag: "AgentControlPersistenceSqlError" } as never);
        }
        return Effect.succeed(
          (
            input?.events ??
            (input?.tasks ?? [task(5)]).flatMap((entry) =>
              entry === "corrupt" ? [] : [taskEvent(entry)],
            )
          )
            .filter((event) => event.sequence > after)
            .slice(0, limit),
        );
      },
      latestSequence: Effect.succeed(1),
    }),
  );

const sqlite = it.layer(NodeSqliteClient.layerMemory());

sqlite("AgentControl task consumer guard", (it) => {
  it.effect("rejects a consistently forged non-canonical task identity before consumption", () =>
    Effect.gen(function* () {
      const canonical = task(5);
      assert.equal(canonical.taskId, yield* deriveAgentControlTaskId(canonical.source));
      const forgedTaskId = AgentControlTaskId.make("task-consumer-guard-forged");
      const forged = { ...canonical, taskId: forgedTaskId };
      const forgedEvent = taskEvent(forged);
      const events = {
        readGlobal: (after = 0, limit = 500) =>
          Effect.succeed([forgedEvent].filter((event) => event.sequence > after).slice(0, limit)),
      };
      const states = {
        listProject: () => Effect.succeed([{ _tag: "Valid" as const, state: forged }] as const),
      };

      const readerResult = yield* Effect.result(
        loadAuthoritativeTaskProjectHistory(projectId, events, states),
      );
      assert.equal(readerResult._tag, "Failure");
      if (readerResult._tag === "Failure") {
        assert.equal(readerResult.failure._tag, "AgentControlProjectionCorruptError");
      }

      let callbackCount = 0;
      const guard = yield* makeGuard({ tasks: [forged], events: [forgedEvent] });
      const guardResult = yield* Effect.result(
        guard.useTaskConsumable(projectId, forgedTaskId, () =>
          Effect.sync(() => {
            callbackCount += 1;
          }),
        ),
      );
      assert.equal(guardResult._tag, "Failure");
      if (guardResult._tag === "Failure") {
        assert.equal(guardResult.failure.reason, "task-projection-corrupt");
      }
      assert.equal(callbackCount, 0);

      assert.deepStrictEqual(
        yield* loadAuthoritativeTaskProjectHistory(
          projectId,
          {
            readGlobal: (after = 0, limit = 500) =>
              Effect.succeed(
                [taskEvent(canonical)].filter((event) => event.sequence > after).slice(0, limit),
              ),
          },
          {
            listProject: () =>
              Effect.succeed([{ _tag: "Valid" as const, state: canonical }] as const),
          },
        ),
        [canonical],
      );
    }),
  );

  it.effect("accepts only a completed, exact current project and task sequence", () =>
    Effect.gen(function* () {
      const guard = yield* makeGuard();
      const current = yield* guard.useTaskConsumable(projectId, task(5).taskId, (_task, gate) =>
        Effect.succeed(gate),
      );
      assert.isTrue(current.sequenceCurrent);
      assert.equal(current.currentSourceSequence, 5);
    }),
  );

  it.effect("future consumer guard rejects every inactive or stale correctness boundary", () =>
    Effect.gen(function* () {
      const cases = [
        { expected: "project-unavailable", input: { available: false } },
        { expected: "mode-inactive", input: { mode: "manual" as const } },
        { expected: "mode-inactive", input: { mode: "paused" as const } },
        { expected: "source-snapshot-unavailable", input: { sourceSequence: null } },
        { expected: "watermark-missing", input: { watermarkStatus: null } },
        {
          expected: "watermark-sequence-mismatch",
          input: { targetSequence: 5, lastCompletedSequence: 4 },
        },
        {
          expected: "watermark-not-completed",
          input: { watermarkStatus: "recovery-required" as const },
        },
        {
          expected: "task-sequence-mismatch",
          input: { sourceSequence: 6 },
        },
        {
          expected: "task-projection-corrupt",
          input: { tasks: ["corrupt" as const] },
        },
      ] as const;

      for (const testCase of cases) {
        const guard = yield* makeGuard(testCase.input);
        const result = yield* Effect.result(
          guard.useTaskConsumable(projectId, task(5).taskId, () => Effect.void),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, testCase.expected);
      }

      const stale = task(4);
      const staleGuard = yield* makeGuard({ tasks: [stale] });
      const taskMismatch = yield* Effect.result(
        staleGuard.useTaskConsumable(projectId, stale.taskId, () => Effect.void),
      );
      assert.equal(taskMismatch._tag, "Failure");
      if (taskMismatch._tag === "Failure") {
        assert.equal(taskMismatch.failure.reason, "task-sequence-mismatch");
      }
    }),
  );

  it.effect("loads the concrete task canonically and rejects every task-local mismatch", () =>
    Effect.gen(function* () {
      const otherProject = ProjectId.make("task-consumer-guard-other");
      const cases = [
        {
          expected: "task-missing",
          taskId: AgentControlTaskId.make("invented-task"),
          input: {},
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: {
            tasks: [{ ...task(5), source: { ...task(5).source, projectId: otherProject } }],
          },
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: { tasks: [{ ...task(5), status: "running" as const }] },
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: { tasks: [{ ...task(5), sourceGate: "paused" as const }] },
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: {
            tasks: [
              {
                ...task(5),
                source: { ...task(5).source, issueNodeId: "not-in-snapshot" },
                sourceSnapshot: {
                  ...task(5).sourceSnapshot,
                  issueNodeId: "not-in-snapshot",
                },
              },
            ],
          },
        },
        {
          expected: "task-source-mismatch",
          taskId: task(5).taskId,
          input: {
            tasks: [
              {
                ...task(5),
                sourceSnapshot: { ...task(5).sourceSnapshot, title: "stale title" },
              },
            ],
          },
        },
        {
          expected: "task-projection-corrupt",
          taskId: task(5).taskId,
          input: { getCorrupt: true },
        },
        {
          expected: "internal-persistence-error",
          taskId: task(5).taskId,
          input: { getSqlError: true },
        },
      ] as const;

      for (const testCase of cases) {
        const guard = yield* makeGuard(testCase.input);
        const result = yield* Effect.result(
          guard.useTaskConsumable(projectId, testCase.taskId, () => Effect.void),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, testCase.expected);
      }
    }),
  );

  it.effect("validates the complete paginated task stream before invoking the callback", () =>
    Effect.gen(function* () {
      const canonical = task(5);
      const healthyEvent = taskEvent(canonical);
      const eventOnlyState = {
        ...canonical,
        source: {
          ...canonical.source,
          issueNodeId: "event-only-issue",
          issueNumber: 2,
          issueUrl: "https://example.test/issues/2",
        },
        sourceSnapshot: {
          ...canonical.sourceSnapshot,
          issueNodeId: "event-only-issue",
          number: 2,
          url: "https://example.test/issues/2",
        },
        taskId: yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: canonical.source.repositoryNodeId,
          issueNodeId: "event-only-issue",
        }),
        sequence: 2,
      };
      const corruptCases = [
        {
          tasks: [canonical],
          events: [
            {
              ...healthyEvent,
              aggregateId: AgentControlTaskId.make("wrong-aggregate"),
            },
          ],
        },
        {
          tasks: [canonical],
          events: [healthyEvent, taskEvent(eventOnlyState)],
        },
        {
          tasks: [canonical],
          events: [healthyEvent, { ...healthyEvent, sequence: 2, streamVersion: 3 }],
        },
        {
          tasks: [
            {
              ...canonical,
              sourceSnapshot: {
                ...canonical.sourceSnapshot,
                title: "projection-only-title",
              },
            },
          ],
          events: [healthyEvent],
        },
      ] as const;

      for (const corruption of corruptCases) {
        let callbackCount = 0;
        const guard = yield* makeGuard(corruption);
        const result = yield* Effect.result(
          guard.useTaskConsumable(projectId, canonical.taskId, () =>
            Effect.sync(() => {
              callbackCount += 1;
            }),
          ),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.reason, "task-projection-corrupt");
        }
        assert.equal(callbackCount, 0);
      }

      const manyTasks = yield* Effect.forEach(
        Array.from({ length: 501 }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            if (index === 0) return canonical;
            const number = index + 1;
            const issueNodeId = `issue-page-${number}`;
            return {
              ...canonical,
              source: {
                ...canonical.source,
                issueNodeId,
                issueNumber: number,
                issueUrl: `https://example.test/issues/${number}`,
              },
              sourceSnapshot: {
                ...canonical.sourceSnapshot,
                issueNodeId,
                number,
                url: `https://example.test/issues/${number}`,
              },
              taskId: yield* deriveAgentControlTaskId({
                projectId,
                repositoryNodeId: canonical.source.repositoryNodeId,
                issueNodeId,
              }),
              sequence: number,
            };
          }),
      );
      const paginated = yield* makeGuard({
        tasks: manyTasks,
        events: manyTasks.map(taskEvent),
      });
      let callbackCount = 0;
      yield* paginated.useTaskConsumable(projectId, canonical.taskId, () =>
        Effect.sync(() => {
          callbackCount += 1;
        }),
      );
      assert.equal(callbackCount, 1);
    }),
  );

  it.effect(
    "keeps project inspection current with zero tasks and composes a claim in an outer transaction",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const emptyGuard = yield* makeGuard({ tasks: [] });
        const empty = yield* emptyGuard.inspectProject(projectId);
        assert.isTrue(empty.sequenceCurrent);
        const missing = yield* Effect.result(
          emptyGuard.useTaskConsumable(projectId, task(5).taskId, () => Effect.void),
        );
        assert.equal(missing._tag, "Failure");
        if (missing._tag === "Failure") assert.equal(missing.failure.reason, "task-missing");

        yield* sql`CREATE TABLE task_claim_probe (task_id TEXT PRIMARY KEY)`;
        const guard = yield* makeGuard({
          issues: [
            issue,
            {
              ...issue,
              issueNodeId: "ineligible-issue",
              number: 2,
              url: "https://example.test/issues/2",
              ready: false,
              eligible: false,
              eligibilityReason: "ready-inactive",
            },
          ],
        });
        yield* sql.withTransaction(
          guard.useTaskConsumable(
            projectId,
            task(5).taskId,
            (canonicalTask) =>
              sql`INSERT INTO task_claim_probe (task_id) VALUES (${canonicalTask.taskId})`,
          ),
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM task_claim_probe
          `)[0]?.count,
          1,
        );
      }),
  );

  it.effect("interrupts an unjoined child before the guarded transaction can commit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_escape (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const childStarted = yield* Deferred.make<void>();
      const childFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Deferred.await(release).pipe(
          Effect.andThen(
            sql`INSERT INTO task_claim_escape (task_id) VALUES (${canonicalTask.taskId})`,
          ),
          Effect.ensuring(Deferred.succeed(childFinalized, undefined).pipe(Effect.ignore)),
          Effect.forkChild({ startImmediately: true }),
          Effect.tap(() => Deferred.succeed(childStarted, undefined)),
          Effect.asVoid,
        ),
      );
      yield* Deferred.await(childStarted);
      assert.isTrue(yield* Deferred.isDone(childFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_escape
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("commits a child claim that the callback explicitly joins", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_joined (task_id TEXT PRIMARY KEY)`;
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Effect.gen(function* () {
          const claim = yield* sql`
            INSERT INTO task_claim_joined (task_id) VALUES (${canonicalTask.taskId})
          `.pipe(Effect.forkChild({ startImmediately: true }));
          yield* Fiber.join(claim);
        }),
      );

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_joined
        `)[0]?.count,
        1,
      );
    }),
  );

  it.effect("terminates attached descendant fibers before committing", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_descendant (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const descendantStarted = yield* Deferred.make<void>();
      const descendantFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      yield* guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
        Effect.gen(function* () {
          yield* Deferred.await(release).pipe(
            Effect.andThen(
              sql`INSERT INTO task_claim_descendant (task_id) VALUES (${canonicalTask.taskId})`,
            ),
            Effect.ensuring(Deferred.succeed(descendantFinalized, undefined).pipe(Effect.ignore)),
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Deferred.succeed(descendantStarted, undefined);
          return yield* Effect.never;
        }).pipe(Effect.forkChild({ startImmediately: true }), Effect.asVoid),
      );
      yield* Deferred.await(descendantStarted);
      assert.isTrue(yield* Deferred.isDone(descendantFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_descendant
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("rolls back a joined child write when the callback fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_rollback (task_id TEXT PRIMARY KEY)`;
      const guard = yield* makeGuard();

      const result = yield* Effect.result(
        guard.useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
          Effect.gen(function* () {
            const claim = yield* sql`
              INSERT INTO task_claim_rollback (task_id) VALUES (${canonicalTask.taskId})
            `.pipe(Effect.forkChild({ startImmediately: true }));
            yield* Fiber.join(claim);
            return yield* Effect.fail("callback-failed");
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_rollback
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("interrupts callback descendants before an interrupted transaction ends", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE task_claim_interrupted (task_id TEXT PRIMARY KEY)`;
      const release = yield* Deferred.make<void>();
      const childStarted = yield* Deferred.make<void>();
      const childFinalized = yield* Deferred.make<void>();
      const guard = yield* makeGuard();

      const guarded = yield* guard
        .useTaskConsumable(projectId, task(5).taskId, (canonicalTask) =>
          Effect.gen(function* () {
            yield* Deferred.await(release).pipe(
              Effect.andThen(
                sql`INSERT INTO task_claim_interrupted (task_id) VALUES (${canonicalTask.taskId})`,
              ),
              Effect.ensuring(Deferred.succeed(childFinalized, undefined).pipe(Effect.ignore)),
              Effect.forkChild({ startImmediately: true }),
            );
            yield* Deferred.succeed(childStarted, undefined);
            return yield* Effect.never;
          }),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(childStarted);
      yield* Fiber.interrupt(guarded);
      assert.isTrue(yield* Deferred.isDone(childFinalized));
      yield* Deferred.succeed(release, undefined);
      yield* Effect.yieldNow;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM task_claim_interrupted
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("admits only the exact selected active Run-Once task and fails closed on races", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 63 });
      const seeded = yield* seedSelectedRunOnceTask(sql);
      const inputs = {
        sourceSequence: seeded.githubSequence,
        targetSequence: seeded.githubSequence,
        lastCompletedSequence: seeded.githubSequence,
        sourceProjectionRevision: 2,
        sourceConfigRevision: 2,
        tasks: [seeded.selectedTask],
        events: [taskEvent(seeded.selectedTask)],
        issues: [issue],
      } as const;
      let callbackCount = 0;
      const guard = yield* makeGuard(inputs);
      const useSelected = guard.useTaskSelectedForRunOnce!;
      const accepted = yield* useSelected(
        seeded.runId,
        projectId,
        seeded.selectedTask.taskId,
        (selected, gate) =>
          Effect.sync(() => {
            callbackCount += 1;
            return { selected, gate };
          }),
      );
      assert.equal(accepted.selected.taskId, seeded.selectedTask.taskId);
      assert.equal(accepted.gate.currentSourceSequence, seeded.githubSequence);
      assert.equal(callbackCount, 1);

      const wrongRun = yield* Effect.result(
        useSelected(
          AgentControlRunOnceId.make(`${seeded.runId}-divergent`),
          projectId,
          seeded.selectedTask.taskId,
          () =>
            Effect.sync(() => {
              callbackCount += 1;
            }),
        ),
      );
      assert.equal(wrongRun._tag, "Failure");
      if (wrongRun._tag === "Failure") assert.equal(wrongRun.failure.reason, "mode-inactive");

      yield* sql`
        UPDATE main.agent_control_project_states
        SET mode = 'paused', paused_from_mode = 'run-once', revision = 3
        WHERE project_id = ${projectId}
      `;
      const paused = yield* Effect.result(
        useSelected(seeded.runId, projectId, seeded.selectedTask.taskId, () =>
          Effect.sync(() => {
            callbackCount += 1;
          }),
        ),
      );
      assert.equal(paused._tag, "Failure");
      if (paused._tag === "Failure") assert.equal(paused.failure.reason, "mode-inactive");
      yield* sql`
        UPDATE main.agent_control_project_states
        SET mode = 'run-once', paused_from_mode = NULL, revision = 2
        WHERE project_id = ${projectId}
      `;

      const assertForeignActivationSupersedesOldRun = Effect.gen(function* () {
        for (const transition of [
          {
            eventId: "task-guard-manual-takeover-event",
            commandId: "task-guard-manual-takeover-command",
            streamVersion: 3,
            previousMode: "run-once",
            mode: "manual",
          },
          {
            eventId: "task-guard-new-observe-event",
            commandId: "task-guard-new-observe-command",
            streamVersion: 4,
            previousMode: "manual",
            mode: "observe",
          },
          {
            eventId: "task-guard-new-run-once-event",
            commandId: "task-guard-new-run-once-command",
            streamVersion: 5,
            previousMode: "observe",
            mode: "run-once",
          },
        ] as const) {
          yield* insertAuthorityEvent(sql, {
            eventId: transition.eventId,
            aggregateKind: "project-controller",
            streamId: projectId,
            streamVersion: transition.streamVersion,
            eventType: "agentControl.project.mode.changed",
            commandId: transition.commandId,
            authority: "human",
            payload: {
              projectId,
              previousMode: transition.previousMode,
              mode: transition.mode,
              previousPausedFromMode: null,
              pausedFromMode: null,
              changedAt: at,
            },
          });
        }
        yield* sql`
        UPDATE main.agent_control_project_states
        SET mode = 'run-once', paused_from_mode = NULL, revision = 5,
          last_event_sequence = (
            SELECT sequence FROM main.agent_control_events
            WHERE event_id = 'task-guard-new-run-once-event'
          )
        WHERE project_id = ${projectId}
      `;
        const supersededRun = yield* Effect.result(
          useSelected(seeded.runId, projectId, seeded.selectedTask.taskId, () =>
            Effect.sync(() => {
              callbackCount += 1;
            }),
          ),
        );
        assert.equal(supersededRun._tag, "Failure");
        if (supersededRun._tag === "Failure") {
          assert.equal(supersededRun.failure.reason, "mode-inactive");
        }
      });

      const staleWatermarkGuard = yield* makeGuard({ ...inputs, watermarkRevision: 2 });
      const staleWatermark = yield* Effect.result(
        staleWatermarkGuard.useTaskSelectedForRunOnce!(
          seeded.runId,
          projectId,
          seeded.selectedTask.taskId,
          () =>
            Effect.sync(() => {
              callbackCount += 1;
            }),
        ),
      );
      assert.equal(staleWatermark._tag, "Failure");
      if (staleWatermark._tag === "Failure") {
        assert.equal(staleWatermark.failure.reason, "watermark-not-completed");
      }

      const divergentSourceGuard = yield* makeGuard({
        ...inputs,
        sourceProjectionRevision: 3,
      });
      const divergentSource = yield* Effect.result(
        divergentSourceGuard.useTaskSelectedForRunOnce!(
          seeded.runId,
          projectId,
          seeded.selectedTask.taskId,
          () =>
            Effect.sync(() => {
              callbackCount += 1;
            }),
        ),
      );
      assert.equal(divergentSource._tag, "Failure");
      if (divergentSource._tag === "Failure") {
        assert.equal(divergentSource.failure.reason, "task-source-mismatch");
      }
      yield* assertForeignActivationSupersedesOldRun;
      assert.equal(callbackCount, 1);
    }),
  );
});
