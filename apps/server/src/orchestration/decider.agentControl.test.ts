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
  type AgentControlThreadControlState,
  type OrchestrationCommand,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import type { OrchestrationCommandAuthority } from "./CommandAuthority.ts";
import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-07-21T12:00:00.000Z";
const PROJECT_ID = ProjectId.make("project-agent-control");
const THREAD_ID = ThreadId.make("thread-agent-control");

const binding = (controlState: AgentControlThreadControlState) => ({
  taskId: AgentControlTaskId.make("task-1"),
  stageRunId: AgentControlStageRunId.make("stage-run-1"),
  attemptId: AgentControlAttemptId.make("attempt-1"),
  roleId: AgentControlRoleId.make("role-implementer"),
  controlState,
});

function readModel(controlState?: AgentControlThreadControlState): OrchestrationReadModel {
  return {
    snapshotSequence: 2,
    projects: [
      {
        id: PROJECT_ID,
        title: "Agent Control",
        workspaceRoot: "/tmp/agent-control",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
    ],
    threads: [
      {
        id: THREAD_ID,
        projectId: PROJECT_ID,
        title: "Controlled thread",
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.4",
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        ...(controlState === undefined ? {} : { agentControl: binding(controlState) }),
        latestTurn: null,
        createdAt: NOW,
        updatedAt: NOW,
        archivedAt: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        settledOverride: null,
        settledAt: null,
        session: null,
      },
    ],
    updatedAt: NOW,
  };
}

const protectedClientCommands: ReadonlyArray<readonly [string, () => OrchestrationCommand]> = [
  [
    "delete",
    () => ({ type: "thread.delete", commandId: CommandId.make("cmd-delete"), threadId: THREAD_ID }),
  ],
  [
    "archive",
    () => ({
      type: "thread.archive",
      commandId: CommandId.make("cmd-archive"),
      threadId: THREAD_ID,
    }),
  ],
  [
    "unarchive",
    () => ({
      type: "thread.unarchive",
      commandId: CommandId.make("cmd-unarchive"),
      threadId: THREAD_ID,
    }),
  ],
  [
    "meta",
    () => ({
      type: "thread.meta.update",
      commandId: CommandId.make("cmd-meta"),
      threadId: THREAD_ID,
      title: "Changed",
    }),
  ],
  [
    "runtime mode",
    () => ({
      type: "thread.runtime-mode.set",
      commandId: CommandId.make("cmd-runtime"),
      threadId: THREAD_ID,
      runtimeMode: "approval-required",
      createdAt: NOW,
    }),
  ],
  [
    "interaction mode",
    () => ({
      type: "thread.interaction-mode.set",
      commandId: CommandId.make("cmd-interaction"),
      threadId: THREAD_ID,
      interactionMode: "plan",
      createdAt: NOW,
    }),
  ],
  [
    "turn start",
    () => ({
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-turn-start"),
      threadId: THREAD_ID,
      message: {
        messageId: MessageId.make("message-turn-start"),
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
      commandId: CommandId.make("cmd-approval"),
      threadId: THREAD_ID,
      requestId: ApprovalRequestId.make("approval-1"),
      decision: "accept",
      createdAt: NOW,
    }),
  ],
  [
    "user input",
    () => ({
      type: "thread.user-input.respond",
      commandId: CommandId.make("cmd-user-input"),
      threadId: THREAD_ID,
      requestId: ApprovalRequestId.make("input-1"),
      answers: { answer: "yes" },
      createdAt: NOW,
    }),
  ],
  [
    "interrupt",
    () => ({
      type: "thread.turn.interrupt",
      commandId: CommandId.make("cmd-interrupt"),
      threadId: THREAD_ID,
      turnId: TurnId.make("turn-1"),
      createdAt: NOW,
    }),
  ],
  [
    "checkpoint revert",
    () => ({
      type: "thread.checkpoint.revert",
      commandId: CommandId.make("cmd-revert"),
      threadId: THREAD_ID,
      turnCount: 0,
      createdAt: NOW,
    }),
  ],
  [
    "session stop",
    () => ({
      type: "thread.session.stop",
      commandId: CommandId.make("cmd-session-stop"),
      threadId: THREAD_ID,
      createdAt: NOW,
    }),
  ],
];

const systemIngestionCommands: ReadonlyArray<readonly [string, () => OrchestrationCommand]> = [
  [
    "session ingestion",
    () => ({
      type: "thread.session.set",
      commandId: CommandId.make("cmd-session-set"),
      threadId: THREAD_ID,
      session: {
        threadId: THREAD_ID,
        status: "running",
        providerName: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access",
        activeTurnId: TurnId.make("turn-system"),
        lastError: null,
        updatedAt: NOW,
      },
      createdAt: NOW,
    }),
  ],
  [
    "assistant delta",
    () => ({
      type: "thread.message.assistant.delta",
      commandId: CommandId.make("cmd-delta"),
      threadId: THREAD_ID,
      messageId: MessageId.make("message-assistant"),
      delta: "partial",
      turnId: TurnId.make("turn-system"),
      createdAt: NOW,
    }),
  ],
  [
    "assistant completion",
    () => ({
      type: "thread.message.assistant.complete",
      commandId: CommandId.make("cmd-complete"),
      threadId: THREAD_ID,
      messageId: MessageId.make("message-assistant"),
      turnId: TurnId.make("turn-system"),
      createdAt: NOW,
    }),
  ],
  [
    "plan ingestion",
    () => ({
      type: "thread.proposed-plan.upsert",
      commandId: CommandId.make("cmd-plan"),
      threadId: THREAD_ID,
      proposedPlan: {
        id: "plan-1",
        turnId: TurnId.make("turn-system"),
        planMarkdown: "Plan",
        implementedAt: null,
        implementationThreadId: null,
        createdAt: NOW,
        updatedAt: NOW,
      },
      createdAt: NOW,
    }),
  ],
  [
    "turn diff finalization",
    () => ({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make("cmd-diff"),
      threadId: THREAD_ID,
      turnId: TurnId.make("turn-system"),
      completedAt: NOW,
      checkpointRef: CheckpointRef.make("checkpoint-1"),
      status: "ready",
      files: [],
      checkpointTurnCount: 1,
      createdAt: NOW,
    }),
  ],
  [
    "activity ingestion",
    () => ({
      type: "thread.activity.append",
      commandId: CommandId.make("cmd-activity"),
      threadId: THREAD_ID,
      activity: {
        id: EventId.make("activity-1"),
        tone: "info",
        kind: "test",
        summary: "Test",
        payload: {},
        turnId: TurnId.make("turn-system"),
        createdAt: NOW,
      },
      createdAt: NOW,
    }),
  ],
  [
    "revert finalization",
    () => ({
      type: "thread.revert.complete",
      commandId: CommandId.make("cmd-revert-complete"),
      threadId: THREAD_ID,
      turnCount: 0,
      createdAt: NOW,
    }),
  ],
];

it.layer(NodeServices.layer)("Agent Control decider rules", (it) => {
  for (const [name, makeCommand] of protectedClientCommands) {
    it.effect(`rejects controlled client ${name}`, () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          decideOrchestrationCommand({
            authority: "client",
            command: makeCommand(),
            readModel: readModel("controlled"),
          }),
        );
        expect(error.message).toContain("controlled by Agent Control");
      }),
    );
  }

  for (const decision of [
    "accept",
    "decline",
    "cancel",
    "acceptForSession",
    "acceptAlways",
  ] as const) {
    it.effect(`limits controlled approval ${decision} to the current pending request`, () =>
      Effect.gen(function* () {
        for (const state of ["pending", "resolved", "foreign-turn"] as const) {
          const model = readModel("controlled");
          const controlled = model.threads[0]!;
          const turnId = TurnId.make("approval-turn");
          const requestId = ApprovalRequestId.make("approval-pending");
          const requested = {
            id: EventId.make("approval-requested"),
            kind: "approval.requested",
            tone: "approval" as const,
            summary: "Approval requested",
            payload: { requestId },
            turnId: state === "foreign-turn" ? TurnId.make("other-turn") : turnId,
            createdAt: NOW,
          };
          const result = yield* Effect.result(
            decideOrchestrationCommand({
              authority: "client",
              command: {
                type: "thread.approval.respond",
                commandId: CommandId.make(`approval-${decision}-${state}`),
                threadId: THREAD_ID,
                requestId,
                decision,
                createdAt: NOW,
              },
              readModel: {
                ...model,
                threads: [
                  {
                    ...controlled,
                    session: {
                      threadId: THREAD_ID,
                      status: "running",
                      providerName: "codex",
                      providerInstanceId: ProviderInstanceId.make("codex"),
                      runtimeMode: "approval-required",
                      activeTurnId: turnId,
                      lastError: null,
                      updatedAt: NOW,
                    },
                    activities:
                      state === "resolved"
                        ? [
                            requested,
                            {
                              ...requested,
                              id: EventId.make("approval-resolved"),
                              kind: "approval.resolved",
                            },
                          ]
                        : [requested],
                  },
                ],
              },
            }),
          );
          const allowed =
            state === "pending" && decision !== "acceptForSession" && decision !== "acceptAlways";
          expect(result._tag).toBe(allowed ? "Success" : "Failure");
        }
      }),
    );
  }

  it.effect("rejects indirect controlled-thread deletion through project.delete", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          authority: "client",
          command: {
            type: "project.delete",
            commandId: CommandId.make("cmd-project-delete"),
            projectId: PROJECT_ID,
            force: true,
          },
          readModel: readModel("controlled"),
        }),
      );
      expect(error.message).toContain("client project deletion is forbidden");
    }),
  );

  for (const [name, makeCommand] of systemIngestionCommands) {
    it.effect(`allows system ${name} on a controlled thread`, () =>
      Effect.gen(function* () {
        const result = yield* decideOrchestrationCommand({
          authority: "system",
          command: makeCommand(),
          readModel: readModel("controlled"),
        });
        expect(Array.isArray(result) ? result.length : 1).toBeGreaterThan(0);
      }),
    );
  }

  for (const authority of [
    "client",
    "system",
  ] satisfies ReadonlyArray<OrchestrationCommandAuthority>) {
    for (const [name, command] of [
      [
        "bind",
        {
          type: "thread.agent-control.bind",
          commandId: CommandId.make(`cmd-bind-${authority}`),
          threadId: THREAD_ID,
          binding: binding("controlled"),
          createdAt: NOW,
        },
      ],
      [
        "state change",
        {
          type: "thread.agent-control.state.set",
          commandId: CommandId.make(`cmd-state-${authority}`),
          threadId: THREAD_ID,
          controlState: "taken-over",
          createdAt: NOW,
        },
      ],
    ] satisfies ReadonlyArray<readonly [string, OrchestrationCommand]>) {
      it.effect(`rejects ${authority} authority spoofing for Agent Control ${name}`, () =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(
            decideOrchestrationCommand({
              authority,
              command,
              readModel: name === "bind" ? readModel() : readModel("controlled"),
            }),
          );
          expect(error.message).toContain("requires 'agent-control' authority");
        }),
      );
    }
  }

  for (const [from, to, allowed] of [
    ["controlled", "controlled", false],
    ["controlled", "taken-over", true],
    ["controlled", "closed", true],
    ["taken-over", "controlled", false],
    ["taken-over", "taken-over", false],
    ["taken-over", "closed", true],
    ["closed", "controlled", false],
    ["closed", "taken-over", false],
    ["closed", "closed", false],
  ] satisfies ReadonlyArray<
    readonly [AgentControlThreadControlState, AgentControlThreadControlState, boolean]
  >) {
    it.effect(`${allowed ? "allows" : "rejects"} ${from} -> ${to}`, () =>
      Effect.gen(function* () {
        const effect = decideOrchestrationCommand({
          authority: "agent-control",
          command: {
            type: "thread.agent-control.state.set",
            commandId: CommandId.make(`cmd-transition-${from}-${to}`),
            threadId: THREAD_ID,
            controlState: to,
            createdAt: NOW,
          },
          readModel: readModel(from),
        });
        if (allowed) {
          const result = yield* effect;
          expect(("type" in result ? result : result[0])?.type).toBe(
            "thread.agent-control-state-set",
          );
          return;
        }
        const error = yield* Effect.flip(effect);
        expect(error.message).toContain("is not allowed");
      }),
    );
  }

  it.effect("lets Agent Control work while controlled and blocks it after takeover", () =>
    Effect.gen(function* () {
      const makeTurn = () => protectedClientCommands.find(([name]) => name === "turn start")![1]();
      yield* decideOrchestrationCommand({
        authority: "agent-control",
        command: makeTurn(),
        readModel: readModel("controlled"),
      });
      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          authority: "agent-control",
          command: makeTurn(),
          readModel: readModel("taken-over"),
        }),
      );
      expect(error.message).toContain("may not issue regular thread commands");
    }),
  );

  it.effect("restores client work after takeover and leaves manual threads unchanged", () =>
    Effect.gen(function* () {
      for (const model of [readModel("taken-over"), readModel()]) {
        const result = yield* decideOrchestrationCommand({
          authority: "client",
          command: {
            type: "thread.meta.update",
            commandId: CommandId.make(
              `cmd-client-meta-${model.threads[0]?.agentControl?.controlState ?? "manual"}`,
            ),
            threadId: THREAD_ID,
            title: "Client title",
          },
          readModel: model,
        });
        expect(("type" in result ? result : result[0])?.type).toBe("thread.meta-updated");
      }
    }),
  );
});

it.layer(NodeServices.layer)("native terminal recovery ownership", (it) => {
  it.effect.each(
    [
      ["controlled", "old-turn", true],
      ["controlled", null, true],
      ["controlled", "new-turn", false],
      ["taken-over", "new-turn", false],
      ["taken-over", null, false],
      ["closed", null, false],
    ].map(([controlState, activeTurnId, accepted]) => ({
      controlState,
      activeTurnId,
      accepted,
    })) as ReadonlyArray<{
      controlState: AgentControlThreadControlState;
      activeTurnId: string | null;
      accepted: boolean;
    }>,
  )(
    "checks current control=$controlState activeTurn=$activeTurnId atomically",
    ({ controlState, activeTurnId, accepted }) =>
      Effect.gen(function* () {
        const model = readModel(controlState);
        const session = {
          threadId: THREAD_ID,
          providerName: "codex" as const,
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "approval-required" as const,
          status: "running" as const,
          activeTurnId: activeTurnId === null ? null : TurnId.make(activeTurnId),
          lastError: null,
          updatedAt: NOW,
        };
        const result = yield* decideOrchestrationCommand({
          authority: "system",
          readModel: { ...model, threads: model.threads.map((thread) => ({ ...thread, session })) },
          command: {
            type: "thread.session.set",
            commandId: CommandId.make("provider:recovery:terminal"),
            threadId: THREAD_ID,
            session: { ...session, status: "ready", activeTurnId: null },
            createdAt: NOW,
            agentControlRecovery: binding("controlled"),
            providerRuntimeLifecycle: {
              runtimeEventId: EventId.make("recovered-terminal"),
              runtimeEventType: "turn.completed",
              providerInstanceId: session.providerInstanceId,
              providerTurnId: TurnId.make("old-turn"),
              providerState: "interrupted",
            },
          },
        }).pipe(Effect.result);
        expect(result._tag).toBe(accepted ? "Success" : "Failure");
        expect(session.activeTurnId).toBe(activeTurnId);
      }),
  );
});
