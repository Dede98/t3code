import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  ApprovalRequestId,
  CheckpointRef,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type AgentControlThreadMaterializeCommand,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../agentControl/controlledThreadReservation/identity.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../agentControl/stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "../../agentControl/stageRunLease/identity.ts";
import { fingerprintAgentControlThreadMaterializationCommand } from "../agentControlThreadMaterializationIntent.ts";

const NOW = "2026-07-21T12:00:00.000Z";
const PROJECT_ID = ProjectId.make("project-agent-control-engine");
const THREAD_ID = ThreadId.make("thread-agent-control-engine");

const binding = {
  taskId: AgentControlTaskId.make("task-engine"),
  stageRunId: AgentControlStageRunId.make("stage-run-engine"),
  attemptId: AgentControlAttemptId.make("attempt-engine"),
  roleId: AgentControlRoleId.make("role-engine"),
  controlState: "controlled" as const,
};

const makeMaterializationCommand = Effect.fn("makeMaterializationEngineCommand")(function* (
  commandId = "cmd-engine-materialize",
) {
  const taskId = AgentControlTaskId.make(`task-engine-materialize-${commandId}`);
  const taskRevision = 3;
  const githubIntakeSequence = 8;
  const sourceIdentityFingerprint = "b".repeat(64);
  const stageKind = "planning" as const;
  const stageOrdinal = 1;
  const attemptOrdinal = 1;
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId: PROJECT_ID,
    taskId,
    taskRevision,
    githubIntakeSequence,
    sourceIdentityFingerprint,
    stageKind,
    stageOrdinal,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, attemptOrdinal);
  const stable = {
    projectId: PROJECT_ID,
    taskId,
    taskRevision,
    githubIntakeSequence,
    sourceIdentityFingerprint,
    stageRunId,
    attemptId,
    roleId: AgentControlRoleId.make("planning"),
    stageKind,
    stageOrdinal,
    attemptOrdinal,
  };
  return {
    type: "thread.agent-control.materialize",
    commandId: CommandId.make(commandId),
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: yield* deriveAgentControlStageRunLeaseId({
      projectId: PROJECT_ID,
      taskId,
    }),
    fenceToken: 5,
    worktreeReservationId: AgentControlWorktreeReservationId.make(
      `worktree-engine-materialize-${commandId}`,
    ),
    title: "Controlled planning thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6",
    },
    runtimeMode: "approval-required",
    interactionMode: "plan",
    branch: "t3-auto/materialize",
    worktreePath: "/tmp/t3-auto/materialize",
    binding: {
      taskId: stable.taskId,
      stageRunId: stable.stageRunId,
      attemptId: stable.attemptId,
      roleId: stable.roleId,
      controlState: "controlled",
    },
    createdAt: NOW,
  } satisfies AgentControlThreadMaterializeCommand;
});

const seedMaterializationProject = Effect.fn("seedMaterializationProject")(function* () {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("cmd-engine-materialize-project"),
    projectId: PROJECT_ID,
    title: "Materialization project",
    workspaceRoot: "/tmp/materialization-project",
    createdAt: NOW,
  });
});

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-agent-control-engine-test-",
});
const commandReceiptLayer = OrchestrationCommandReceiptRepositoryLive;
const testLayer = Layer.mergeAll(
  OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
    Layer.provide(commandReceiptLayer),
  ),
  OrchestrationProjectionSnapshotQueryLive,
  commandReceiptLayer,
).pipe(
  Layer.provide(OrchestrationEventStoreLive),
  Layer.provide(RepositoryIdentityResolver.layer),
  Layer.provide(commandReceiptLayer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(serverConfigLayer),
  Layer.provideMerge(NodeServices.layer),
);

const seedThread = Effect.fn("seedThread")(function* (bind: boolean) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("cmd-engine-project-create"),
    projectId: PROJECT_ID,
    title: "Agent Control Engine",
    workspaceRoot: "/tmp/agent-control-engine",
    createdAt: NOW,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make("cmd-engine-thread-create"),
    threadId: THREAD_ID,
    projectId: PROJECT_ID,
    title: "Engine thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: NOW,
  });
  if (bind) {
    yield* engine.dispatchAgentControl({
      type: "thread.agent-control.bind",
      commandId: CommandId.make("cmd-engine-bind"),
      threadId: THREAD_ID,
      binding,
      createdAt: NOW,
    });
  }
});

const getThreadDetail = Effect.fn("getThreadDetail")(function* () {
  const snapshots = yield* ProjectionSnapshotQuery;
  return Option.getOrNull(yield* snapshots.getThreadDetailById(THREAD_ID));
});

const protectedClientCommands: ReadonlyArray<readonly [string, () => OrchestrationCommand]> = [
  [
    "delete",
    () => ({
      type: "thread.delete",
      commandId: CommandId.make("cmd-engine-delete"),
      threadId: THREAD_ID,
    }),
  ],
  [
    "archive",
    () => ({
      type: "thread.archive",
      commandId: CommandId.make("cmd-engine-archive"),
      threadId: THREAD_ID,
    }),
  ],
  [
    "unarchive",
    () => ({
      type: "thread.unarchive",
      commandId: CommandId.make("cmd-engine-unarchive"),
      threadId: THREAD_ID,
    }),
  ],
  [
    "meta",
    () => ({
      type: "thread.meta.update",
      commandId: CommandId.make("cmd-engine-meta"),
      threadId: THREAD_ID,
      title: "Changed",
    }),
  ],
  [
    "runtime mode",
    () => ({
      type: "thread.runtime-mode.set",
      commandId: CommandId.make("cmd-engine-runtime"),
      threadId: THREAD_ID,
      runtimeMode: "approval-required",
      createdAt: NOW,
    }),
  ],
  [
    "interaction mode",
    () => ({
      type: "thread.interaction-mode.set",
      commandId: CommandId.make("cmd-engine-interaction"),
      threadId: THREAD_ID,
      interactionMode: "plan",
      createdAt: NOW,
    }),
  ],
  [
    "turn start",
    () => ({
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-engine-turn"),
      threadId: THREAD_ID,
      message: {
        messageId: MessageId.make("message-engine-turn"),
        role: "user",
        text: "work",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: NOW,
    }),
  ],
  [
    "approval",
    () => ({
      type: "thread.approval.respond",
      commandId: CommandId.make("cmd-engine-approval"),
      threadId: THREAD_ID,
      requestId: ApprovalRequestId.make("approval-engine"),
      decision: "accept",
      createdAt: NOW,
    }),
  ],
  [
    "user input",
    () => ({
      type: "thread.user-input.respond",
      commandId: CommandId.make("cmd-engine-input"),
      threadId: THREAD_ID,
      requestId: ApprovalRequestId.make("input-engine"),
      answers: { answer: "yes" },
      createdAt: NOW,
    }),
  ],
  [
    "interrupt",
    () => ({
      type: "thread.turn.interrupt",
      commandId: CommandId.make("cmd-engine-interrupt"),
      threadId: THREAD_ID,
      turnId: TurnId.make("turn-engine"),
      createdAt: NOW,
    }),
  ],
  [
    "checkpoint revert",
    () => ({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-engine-revert"),
      threadId: THREAD_ID,
      turnCount: 0,
      createdAt: NOW,
    }),
  ],
  [
    "session stop",
    () => ({
      type: "thread.session.stop",
      commandId: CommandId.make("cmd-engine-stop"),
      threadId: THREAD_ID,
      createdAt: NOW,
    }),
  ],
];

describe("OrchestrationEngine Agent Control", () => {
  it.effect(
    "reserves the future Agent Control thread namespace before receipt replay for every authority",
    () =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const receipts = yield* OrchestrationCommandReceiptRepository;
        const sql = yield* SqlClient.SqlClient;
        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("cmd-reserved-prefix-project"),
          projectId: PROJECT_ID,
          title: "Reserved prefix",
          workspaceRoot: "/tmp/reserved-prefix",
          createdAt: NOW,
        });
        const makeCreate = (commandId: string, threadId: string) =>
          ({
            type: "thread.create",
            commandId: CommandId.make(commandId),
            threadId: ThreadId.make(threadId),
            projectId: PROJECT_ID,
            title: "Thread",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5.4",
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: NOW,
          }) as const;

        const reserved = makeCreate("cmd-reserved-prefix-client", "t3-auto-reserved-thread-exact");
        for (const [index, dispatch] of [
          engine.dispatchClient,
          engine.dispatch,
          engine.dispatchAgentControl,
        ].entries()) {
          const failure = yield* Effect.flip(
            dispatch({
              ...reserved,
              commandId: CommandId.make(`${reserved.commandId}-${index}`),
            }),
          );
          expect(failure).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
        }

        const replayBypass = makeCreate(
          "cmd-reserved-prefix-receipt",
          "t3-auto-reserved-thread-receipt",
        );
        yield* receipts.insert({
          commandId: replayBypass.commandId,
          authority: "client",
          aggregateKind: "thread",
          aggregateId: replayBypass.threadId,
          acceptedAt: NOW,
          resultSequence: 1,
          status: "accepted",
          error: null,
        });
        const receiptFailure = yield* Effect.flip(engine.dispatchClient(replayBypass));
        expect(receiptFailure).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });

        const manual = makeCreate("cmd-manual-prefix-control", "manual-thread-allowed");
        yield* engine.dispatchClient(manual);
        expect(
          (yield* sql<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM projection_threads
              WHERE thread_id = ${manual.threadId}
            `)[0]!.count,
        ).toBe(1);
        expect(
          (yield* sql<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM orchestration_events
              WHERE stream_id LIKE 't3-auto-reserved-thread-%'
            `)[0]!.count,
        ).toBe(0);
        expect(
          (yield* sql<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM projection_threads
              WHERE thread_id LIKE 't3-auto-reserved-thread-%'
            `)[0]!.count,
        ).toBe(0);
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("rejects every protected client command and fail-closes project.delete", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      yield* seedThread(true);

      for (const [name, makeCommand] of protectedClientCommands) {
        const error = yield* Effect.flip(engine.dispatchClient(makeCommand()));
        expect(error, name).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
      }
      const projectDeleteError = yield* Effect.flip(
        engine.dispatchClient({
          type: "project.delete",
          commandId: CommandId.make("cmd-engine-project-delete"),
          projectId: PROJECT_ID,
          force: true,
        }),
      );
      expect(projectDeleteError).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
      expect((yield* getThreadDetail())?.agentControl).toEqual(binding);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("rejects Agent Control command authority spoofing", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      yield* seedThread(false);

      for (const [label, dispatch] of [
        ["system", engine.dispatch],
        ["client", engine.dispatchClient],
      ] as const) {
        const error = yield* Effect.flip(
          dispatch({
            type: "thread.agent-control.bind",
            commandId: CommandId.make(`cmd-engine-spoof-bind-${label}`),
            threadId: THREAD_ID,
            binding,
            createdAt: NOW,
          }),
        );
        expect(error).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
      }

      yield* engine.dispatchAgentControl({
        type: "thread.agent-control.bind",
        commandId: CommandId.make("cmd-engine-real-bind"),
        threadId: THREAD_ID,
        binding,
        createdAt: NOW,
      });
      for (const [label, dispatch] of [
        ["system", engine.dispatch],
        ["client", engine.dispatchClient],
      ] as const) {
        const error = yield* Effect.flip(
          dispatch({
            type: "thread.agent-control.state.set",
            commandId: CommandId.make(`cmd-engine-spoof-state-${label}`),
            threadId: THREAD_ID,
            controlState: "taken-over",
            createdAt: NOW,
          }),
        );
        expect(error).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
      }
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("keeps system ingestion and turn finalization working while controlled", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      yield* seedThread(true);
      const commands: ReadonlyArray<OrchestrationCommand> = [
        {
          type: "thread.session.set",
          commandId: CommandId.make("cmd-engine-system-session"),
          threadId: THREAD_ID,
          session: {
            threadId: THREAD_ID,
            status: "running",
            providerName: "codex",
            providerInstanceId: ProviderInstanceId.make("codex"),
            runtimeMode: "full-access",
            activeTurnId: TurnId.make("turn-engine-system"),
            lastError: null,
            updatedAt: NOW,
          },
          createdAt: NOW,
        },
        {
          type: "thread.message.assistant.delta",
          commandId: CommandId.make("cmd-engine-system-delta"),
          threadId: THREAD_ID,
          messageId: MessageId.make("message-engine-system"),
          delta: "done",
          turnId: TurnId.make("turn-engine-system"),
          createdAt: NOW,
        },
        {
          type: "thread.message.assistant.complete",
          commandId: CommandId.make("cmd-engine-system-complete"),
          threadId: THREAD_ID,
          messageId: MessageId.make("message-engine-system"),
          turnId: TurnId.make("turn-engine-system"),
          createdAt: NOW,
        },
        {
          type: "thread.turn.diff.complete",
          commandId: CommandId.make("cmd-engine-system-diff"),
          threadId: THREAD_ID,
          turnId: TurnId.make("turn-engine-system"),
          completedAt: NOW,
          checkpointRef: CheckpointRef.make("checkpoint-engine-system"),
          status: "ready",
          files: [],
          checkpointTurnCount: 1,
          createdAt: NOW,
        },
        {
          type: "thread.activity.append",
          commandId: CommandId.make("cmd-engine-system-activity"),
          threadId: THREAD_ID,
          activity: {
            id: EventId.make("activity-engine-system"),
            tone: "info",
            kind: "system.test",
            summary: "System ingestion",
            payload: {},
            turnId: TurnId.make("turn-engine-system"),
            createdAt: NOW,
          },
          createdAt: NOW,
        },
      ];

      for (const command of commands) {
        expect(yield* engine.dispatch(command)).toHaveProperty("sequence");
      }
      const detail = yield* getThreadDetail();
      expect(detail?.agentControl).toEqual(binding);
      expect(detail?.messages).toHaveLength(1);
      expect(detail?.checkpoints).toHaveLength(1);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("projects state transitions to detail and shell and enforces takeover ownership", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const snapshots = yield* ProjectionSnapshotQuery;
      yield* seedThread(true);

      expect((yield* getThreadDetail())?.agentControl).toEqual(binding);
      expect((yield* snapshots.getShellSnapshot()).threads[0]?.agentControl).toEqual(binding);

      yield* engine.dispatchAgentControl({
        type: "thread.agent-control.state.set",
        commandId: CommandId.make("cmd-engine-takeover"),
        threadId: THREAD_ID,
        controlState: "taken-over",
        createdAt: NOW,
      });
      expect(
        yield* engine.dispatchClient({
          type: "thread.meta.update",
          commandId: CommandId.make("cmd-engine-client-after-takeover"),
          threadId: THREAD_ID,
          title: "Taken over",
        }),
      ).toHaveProperty("sequence");
      const controllerError = yield* Effect.flip(
        engine.dispatchAgentControl({
          type: "thread.turn.start",
          commandId: CommandId.make("cmd-engine-controller-after-takeover"),
          threadId: THREAD_ID,
          message: {
            messageId: MessageId.make("message-engine-controller-after-takeover"),
            role: "user",
            text: "continue",
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: NOW,
        }),
      );
      expect(controllerError).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });

      yield* engine.dispatchAgentControl({
        type: "thread.agent-control.state.set",
        commandId: CommandId.make("cmd-engine-close"),
        threadId: THREAD_ID,
        controlState: "closed",
        createdAt: NOW,
      });
      const reopenError = yield* Effect.flip(
        engine.dispatchAgentControl({
          type: "thread.agent-control.state.set",
          commandId: CommandId.make("cmd-engine-reopen"),
          threadId: THREAD_ID,
          controlState: "controlled",
          createdAt: NOW,
        }),
      );
      expect(reopenError).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });

      expect((yield* getThreadDetail())?.agentControl?.controlState).toBe("closed");
      expect((yield* snapshots.getShellSnapshot()).threads[0]?.agentControl?.controlState).toBe(
        "closed",
      );
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect(
    "atomically materializes the reserved thread, binds it, and replays without artifacts",
    () =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const snapshots = yield* ProjectionSnapshotQuery;
        const sql = yield* SqlClient.SqlClient;
        yield* seedMaterializationProject();
        const command = yield* makeMaterializationCommand();
        const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
        const subscription = yield* subscribe;
        const authorityAtPublication: Array<string | undefined> = [];
        const publication = yield* subscription.pipe(
          Stream.take(2),
          Stream.tap(() =>
            snapshots.getThreadDetailById(command.threadId).pipe(
              Effect.tap((detail) =>
                Effect.sync(() => {
                  authorityAtPublication.push(Option.getOrNull(detail)?.agentControl?.controlState);
                }),
              ),
            ),
          ),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Effect.yieldNow;

        const result = yield* engine.dispatchAgentControl(command);
        const events = Array.from(yield* Fiber.join(publication));
        expect(events.map((event) => event.type)).toEqual([
          "thread.created",
          "thread.agent-control-bound",
        ]);
        expect(authorityAtPublication).toEqual(["controlled", "controlled"]);
        expect(result.sequence).toBe(events[1]?.sequence);

        const counts = yield* sql<{
          readonly events: number;
          readonly threads: number;
          readonly intents: number;
          readonly acceptedReceipts: number;
          readonly materializationMarkers: number;
          readonly sessions: number;
          readonly messages: number;
          readonly turns: number;
          readonly providerSessions: number;
          readonly providerCommands: number;
          readonly agentControlEvents: number;
          readonly taskStates: number;
          readonly stageRunStates: number;
          readonly leaseStates: number;
          readonly worktreeStates: number;
          readonly controlledThreadReservationStates: number;
        }>`
          SELECT
            (SELECT COUNT(*) FROM orchestration_events
             WHERE command_id = ${command.commandId}) AS events,
            (SELECT COUNT(*) FROM projection_threads
             WHERE thread_id = ${command.threadId}
               AND json_extract(agent_control_json, '$.controlState') = 'controlled') AS threads,
            (SELECT COUNT(*)
             FROM orchestration_agent_control_thread_materialization_intents
             WHERE command_id = ${command.commandId}
               AND receipt_status = 'accepted') AS intents,
            (SELECT COUNT(*) FROM orchestration_command_receipts
             WHERE command_id = ${command.commandId}
               AND status = 'accepted') AS acceptedReceipts,
            (SELECT COUNT(*)
             FROM orchestration_agent_control_thread_materialization_receipts
             WHERE command_id = ${command.commandId}) AS materializationMarkers,
            (SELECT COUNT(*) FROM projection_thread_sessions
             WHERE thread_id = ${command.threadId}) AS sessions,
            (SELECT COUNT(*) FROM projection_thread_messages
             WHERE thread_id = ${command.threadId}) AS messages,
            (SELECT COUNT(*) FROM projection_turns
             WHERE thread_id = ${command.threadId}) AS turns,
            (SELECT COUNT(*) FROM provider_session_runtime
             WHERE thread_id = ${command.threadId}) AS providerSessions,
            (SELECT COUNT(*) FROM orchestration_events
             WHERE stream_id = ${command.threadId}
               AND event_type IN (
                 'thread.runtime-mode-set',
                 'thread.turn-start-requested',
                 'thread.turn-interrupt-requested',
                 'thread.approval-response-requested',
                 'thread.user-input-response-requested',
                 'thread.session-stop-requested'
               )) AS providerCommands,
            (SELECT COUNT(*) FROM agent_control_events) AS agentControlEvents,
            (SELECT COUNT(*) FROM agent_control_task_states) AS taskStates,
            (SELECT COUNT(*) FROM agent_control_stage_run_states) AS stageRunStates,
            (SELECT COUNT(*) FROM agent_control_stage_run_lease_states) AS leaseStates,
            (SELECT COUNT(*) FROM agent_control_worktree_reservation_states) AS worktreeStates,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_reservation_states)
              AS controlledThreadReservationStates
        `;
        expect(counts[0]).toEqual({
          events: 2,
          threads: 1,
          intents: 1,
          acceptedReceipts: 1,
          materializationMarkers: 1,
          sessions: 0,
          messages: 0,
          turns: 0,
          providerSessions: 0,
          providerCommands: 0,
          agentControlEvents: 0,
          taskStates: 0,
          stageRunStates: 0,
          leaseStates: 0,
          worktreeStates: 0,
          controlledThreadReservationStates: 0,
        });
        const intent = (yield* sql<{ readonly commandFingerprint: string }>`
            SELECT command_fingerprint AS "commandFingerprint"
            FROM orchestration_agent_control_thread_materialization_intents
            WHERE command_id = ${command.commandId}
          `)[0]!;
        expect(intent.commandFingerprint).toMatch(/^[0-9a-f]{64}$/);
        expect(intent.commandFingerprint).toBe(
          yield* fingerprintAgentControlThreadMaterializationCommand(yield* Crypto.Crypto, command),
        );
        const stream = yield* sql<{
          readonly type: string;
          readonly streamVersion: number;
          readonly sequence: number;
        }>`
          SELECT
            event_type AS type,
            stream_version AS "streamVersion",
            sequence
          FROM orchestration_events
          WHERE command_id = ${command.commandId}
          ORDER BY sequence
        `;
        expect(stream.map(({ type, streamVersion }) => ({ type, streamVersion }))).toEqual([
          { type: "thread.created", streamVersion: 1 },
          { type: "thread.agent-control-bound", streamVersion: 2 },
        ]);
        expect(stream[1]?.sequence).toBe(result.sequence);
        const intentCoordinates = yield* sql<{
          readonly createdStreamVersion: number;
          readonly bindingStreamVersion: number;
          readonly receiptResultSequence: number;
          readonly acceptedReceiptCommandId: string;
        }>`
          SELECT
            created_event_stream_version AS "createdStreamVersion",
            binding_event_stream_version AS "bindingStreamVersion",
            receipt_result_sequence AS "receiptResultSequence",
            accepted_receipt_command_id AS "acceptedReceiptCommandId"
          FROM orchestration_agent_control_thread_materialization_intents
          WHERE command_id = ${command.commandId}
        `;
        expect(intentCoordinates).toEqual([
          {
            createdStreamVersion: 1,
            bindingStreamVersion: 2,
            receiptResultSequence: result.sequence,
            acceptedReceiptCommandId: command.commandId,
          },
        ]);

        const replaySubscription = yield* subscribe;
        const replayPublication = yield* Stream.runHead(replaySubscription).pipe(Effect.forkChild);
        expect(yield* engine.dispatchAgentControl(command)).toEqual(result);
        yield* Effect.yieldNow;
        expect(replayPublication.pollUnsafe()).toBeUndefined();
        yield* Fiber.interrupt(replayPublication);
        const replayCounts = yield* sql<{ readonly events: number; readonly intents: number }>`
          SELECT
            (SELECT COUNT(*) FROM orchestration_events
             WHERE command_id = ${command.commandId}) AS events,
            (SELECT COUNT(*)
             FROM orchestration_agent_control_thread_materialization_intents
             WHERE command_id = ${command.commandId}) AS intents
        `;
        expect(replayCounts[0]).toEqual({ events: 2, intents: 1 });
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect(
    "rejects client and system materialization without allowing receipt replay authority",
    () =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const sql = yield* SqlClient.SqlClient;
        yield* seedMaterializationProject();
        const command = yield* makeMaterializationCommand("cmd-engine-materialize-authority");

        for (const dispatch of [engine.dispatchClient, engine.dispatch]) {
          const failure = yield* Effect.flip(dispatch(command));
          expect(failure).toMatchObject({ _tag: "OrchestrationCommandInvariantError" });
        }
        const rowsBefore = yield* sql<{ readonly receipts: number; readonly intents: number }>`
          SELECT
            (SELECT COUNT(*) FROM orchestration_command_receipts
             WHERE command_id = ${command.commandId}) AS receipts,
            (SELECT COUNT(*)
             FROM orchestration_agent_control_thread_materialization_intents
             WHERE command_id = ${command.commandId}) AS intents
        `;
        expect(rowsBefore[0]).toEqual({ receipts: 0, intents: 0 });
        expect(yield* engine.dispatchAgentControl(command)).toHaveProperty("sequence");
      }).pipe(Effect.provide(testLayer)),
  );

  it.effect("binds every replayed command component to the immutable fingerprint", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const sql = yield* SqlClient.SqlClient;
      yield* seedMaterializationProject();
      const command = yield* makeMaterializationCommand("cmd-engine-materialize-fingerprint");
      yield* engine.dispatchAgentControl(command);

      const mutations: ReadonlyArray<AgentControlThreadMaterializeCommand> = [
        {
          ...command,
          controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
            "other-controlled-thread-reservation",
          ),
        },
        { ...command, threadId: ThreadId.make("t3-auto-reserved-thread-other") },
        { ...command, projectId: ProjectId.make("other-project") },
        { ...command, taskId: AgentControlTaskId.make("other-task") },
        { ...command, taskRevision: command.taskRevision + 1 },
        { ...command, githubIntakeSequence: command.githubIntakeSequence + 1 },
        { ...command, sourceIdentityFingerprint: "c".repeat(64) },
        { ...command, stageRunId: AgentControlStageRunId.make("other-stage-run") },
        { ...command, attemptId: AgentControlAttemptId.make("other-attempt") },
        { ...command, roleId: AgentControlRoleId.make("implementation") },
        { ...command, stageKind: "implementation" },
        { ...command, stageOrdinal: 2 },
        { ...command, attemptOrdinal: 2 },
        { ...command, leaseId: AgentControlStageRunLeaseId.make("other-lease") },
        { ...command, fenceToken: command.fenceToken + 1 },
        {
          ...command,
          worktreeReservationId: AgentControlWorktreeReservationId.make(
            "other-worktree-reservation",
          ),
        },
        { ...command, title: "Other title" },
        {
          ...command,
          modelSelection: {
            ...command.modelSelection,
            model: "gpt-5.6-mini",
          },
        },
        {
          ...command,
          modelSelection: {
            ...command.modelSelection,
            options: [{ id: "reasoning-effort", value: "high" }],
          },
        },
        { ...command, runtimeMode: "full-access" },
        { ...command, interactionMode: "default" },
        { ...command, branch: "other-branch" },
        { ...command, worktreePath: "/tmp/other-worktree" },
        {
          ...command,
          binding: {
            ...command.binding,
            taskId: AgentControlTaskId.make("other-binding-task"),
          },
        },
        {
          ...command,
          binding: {
            ...command.binding,
            stageRunId: AgentControlStageRunId.make("other-binding-stage-run"),
          },
        },
        {
          ...command,
          binding: {
            ...command.binding,
            attemptId: AgentControlAttemptId.make("other-binding-attempt"),
          },
        },
        {
          ...command,
          binding: {
            ...command.binding,
            roleId: AgentControlRoleId.make("other-binding-role"),
          },
        },
        {
          ...command,
          binding: { ...command.binding, controlState: "taken-over" },
        },
        { ...command, createdAt: "2026-07-27T10:00:01.000Z" },
      ];
      for (const mutated of mutations) {
        const failure = yield* Effect.flip(engine.dispatchAgentControl(mutated));
        expect(failure).toMatchObject({
          _tag: "OrchestrationCommandIdentityConflictError",
          commandId: command.commandId,
        });
      }
      const changedType = yield* Effect.flip(
        engine.dispatchAgentControl({
          type: "thread.agent-control.bind",
          commandId: command.commandId,
          threadId: command.threadId,
          binding: command.binding,
          createdAt: command.createdAt,
        }),
      );
      expect(changedType).toMatchObject({
        _tag: "OrchestrationCommandIdentityConflictError",
        commandId: command.commandId,
      });
      const counts = yield* sql<{
        readonly events: number;
        readonly receipts: number;
        readonly intents: number;
      }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_events
           WHERE command_id = ${command.commandId}) AS events,
          (SELECT COUNT(*) FROM orchestration_command_receipts
           WHERE command_id = ${command.commandId}) AS receipts,
          (SELECT COUNT(*)
           FROM orchestration_agent_control_thread_materialization_intents
           WHERE command_id = ${command.commandId}) AS intents
      `;
      expect(counts[0]).toEqual({ events: 2, receipts: 1, intents: 1 });
    }).pipe(Effect.provide(testLayer)),
  );
});
