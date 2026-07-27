import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  ApprovalRequestId,
  CheckpointRef,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
});
