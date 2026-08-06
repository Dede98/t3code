import {
  AgentControlRoleId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type AgentControlThreadMaterializeCommand,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../agentControl/controlledThreadReservation/identity.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../agentControl/stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "../agentControl/stageRunLease/identity.ts";
import type { OrchestrationCommandAuthority } from "./CommandAuthority.ts";
import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-07-27T10:00:00.000Z";
const PROJECT_ID = ProjectId.make("materialization-project");

const makeCommand = Effect.fn("makeMaterializationDeciderCommand")(function* () {
  const taskId = AgentControlTaskId.make("materialization-task");
  const taskRevision = 4;
  const githubIntakeSequence = 9;
  const sourceIdentityFingerprint = "a".repeat(64);
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
    commandId: CommandId.make("materialization-command"),
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: yield* deriveAgentControlStageRunLeaseId({
      projectId: PROJECT_ID,
      taskId,
    }),
    fenceToken: 3,
    worktreeReservationId: AgentControlWorktreeReservationId.make("materialization-worktree"),
    title: "Planning thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6",
    },
    runtimeMode: "approval-required",
    interactionMode: "plan",
    branch: "t3-auto/materialization",
    worktreePath: "/tmp/t3-auto/materialization",
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

const makeImplementationCommand = Effect.fn("makeImplementationMaterializationDeciderCommand")(
  function* () {
    const planning = yield* makeCommand();
    const stageKind = "implementation" as const;
    const stageOrdinal = 2;
    const attemptOrdinal = 1;
    const stageRunId = yield* deriveAgentControlStageRunId({
      projectId: planning.projectId,
      taskId: planning.taskId,
      taskRevision: planning.taskRevision,
      githubIntakeSequence: planning.githubIntakeSequence,
      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
      stageKind,
      stageOrdinal,
    });
    const attemptId = yield* deriveAgentControlAttemptId(stageRunId, attemptOrdinal);
    const stable = {
      projectId: planning.projectId,
      taskId: planning.taskId,
      taskRevision: planning.taskRevision,
      githubIntakeSequence: planning.githubIntakeSequence,
      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
      stageRunId,
      attemptId,
      roleId: AgentControlRoleId.make("implementer"),
      stageKind,
      stageOrdinal,
      attemptOrdinal,
    };
    return {
      ...planning,
      commandId: CommandId.make("implementation-materialization-command"),
      controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
      threadId: yield* deriveAgentControlReservedThreadId(stable),
      ...stable,
      interactionMode: "default",
      binding: {
        taskId: stable.taskId,
        stageRunId,
        attemptId,
        roleId: stable.roleId,
        controlState: "controlled",
      },
      sourceProposedPlan: {
        threadId: ThreadId.make("planning-source-thread"),
        planId: "accepted-plan",
      },
    } satisfies AgentControlThreadMaterializeCommand;
  },
);

const makeVerificationCommand = Effect.fn("makeVerificationMaterializationDeciderCommand")(
  function* () {
    const implementation = yield* makeImplementationCommand();
    const stageKind = "verification" as const;
    const stageOrdinal = 3;
    const attemptOrdinal = 1;
    const stageRunId = yield* deriveAgentControlStageRunId({
      projectId: implementation.projectId,
      taskId: implementation.taskId,
      taskRevision: implementation.taskRevision,
      githubIntakeSequence: implementation.githubIntakeSequence,
      sourceIdentityFingerprint: implementation.sourceIdentityFingerprint,
      stageKind,
      stageOrdinal,
    });
    const attemptId = yield* deriveAgentControlAttemptId(stageRunId, attemptOrdinal);
    const stable = {
      projectId: implementation.projectId,
      taskId: implementation.taskId,
      taskRevision: implementation.taskRevision,
      githubIntakeSequence: implementation.githubIntakeSequence,
      sourceIdentityFingerprint: implementation.sourceIdentityFingerprint,
      stageRunId,
      attemptId,
      roleId: AgentControlRoleId.make("verifier"),
      stageKind,
      stageOrdinal,
      attemptOrdinal,
    };
    return {
      ...implementation,
      commandId: CommandId.make("verification-materialization-command"),
      controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
      threadId: yield* deriveAgentControlReservedThreadId(stable),
      ...stable,
      runtimeMode: "approval-required",
      interactionMode: "default",
      binding: {
        taskId: stable.taskId,
        stageRunId,
        attemptId,
        roleId: stable.roleId,
        controlState: "controlled",
      },
    } satisfies AgentControlThreadMaterializeCommand;
  },
);

const readModel = (deletedAt: string | null = null): OrchestrationReadModel => ({
  snapshotSequence: 7,
  projects: [
    {
      id: PROJECT_ID,
      title: "Materialization",
      workspaceRoot: "/tmp/materialization-project",
      defaultModelSelection: null,
      scripts: [],
      createdAt: NOW,
      updatedAt: NOW,
      deletedAt,
    },
  ],
  threads: [],
  updatedAt: NOW,
});

it.layer(NodeServices.layer)("controlled thread materialization decider", (it) => {
  it.effect("emits create and controlled bind under Agent Control authority", () =>
    Effect.gen(function* () {
      const command = yield* makeCommand();
      const result = yield* decideOrchestrationCommand({
        authority: "agent-control",
        command,
        readModel: readModel(),
      });
      assert.isTrue(Array.isArray(result));
      if (!Array.isArray(result)) return;
      assert.deepStrictEqual(
        result.map((event) => event.type),
        ["thread.created", "thread.agent-control-bound"],
      );
      for (const event of result) {
        assert.strictEqual(event.commandId, command.commandId);
        assert.strictEqual(event.aggregateId, command.threadId);
        assert.strictEqual(event.occurredAt, command.createdAt);
      }
      assert.deepStrictEqual(result[1]?.payload, {
        threadId: command.threadId,
        binding: command.binding,
        updatedAt: command.createdAt,
      });
    }),
  );

  it.effect("rejects client and generic system authority", () =>
    Effect.gen(function* () {
      const command = yield* makeCommand();
      for (const authority of [
        "client",
        "system",
      ] satisfies ReadonlyArray<OrchestrationCommandAuthority>) {
        const failure = yield* Effect.flip(
          decideOrchestrationCommand({
            authority,
            command,
            readModel: readModel(),
          }),
        );
        assert.include(failure.message, "requires 'agent-control' authority");
      }
    }),
  );

  it.effect("accepts only the closed Implementation form with an exact source plan", () =>
    Effect.gen(function* () {
      const command = yield* makeImplementationCommand();
      const accepted = yield* decideOrchestrationCommand({
        authority: "agent-control",
        command,
        readModel: readModel(),
      });
      assert.isTrue(Array.isArray(accepted));

      for (const mutation of [
        { ...command, roleId: AgentControlRoleId.make("planning") },
        { ...command, stageKind: "planning" as const },
        { ...command, stageOrdinal: 1 },
        { ...command, attemptOrdinal: 2 },
        { ...command, interactionMode: "plan" as const },
        { ...command, sourceProposedPlan: undefined },
      ] satisfies ReadonlyArray<AgentControlThreadMaterializeCommand>) {
        assert.strictEqual(
          (yield* Effect.exit(
            decideOrchestrationCommand({
              authority: "agent-control",
              command: mutation,
              readModel: readModel(),
            }),
          ))._tag,
          "Failure",
        );
      }
    }),
  );

  it.effect("accepts only the closed Verification form with its original source plan", () =>
    Effect.gen(function* () {
      const command = yield* makeVerificationCommand();
      const accepted = yield* decideOrchestrationCommand({
        authority: "agent-control",
        command,
        readModel: readModel(),
      });
      assert.isTrue(Array.isArray(accepted));

      for (const mutation of [
        { ...command, roleId: AgentControlRoleId.make("implementer") },
        { ...command, stageKind: "implementation" as const },
        { ...command, stageOrdinal: 2 },
        { ...command, attemptOrdinal: 2 },
        { ...command, runtimeMode: "full-access" as const },
        { ...command, interactionMode: "plan" as const },
        { ...command, sourceProposedPlan: undefined },
      ] satisfies ReadonlyArray<AgentControlThreadMaterializeCommand>) {
        assert.strictEqual(
          (yield* Effect.exit(
            decideOrchestrationCommand({
              authority: "agent-control",
              command: mutation,
              readModel: readModel(),
            }),
          ))._tag,
          "Failure",
        );
      }
    }),
  );

  it.effect("enforces canonical identity, planning, binding, project and thread absence", () =>
    Effect.gen(function* () {
      const command = yield* makeCommand();
      const mutations: ReadonlyArray<AgentControlThreadMaterializeCommand> = [
        {
          ...command,
          controlledThreadReservationId:
            `${command.controlledThreadReservationId}-other` as AgentControlThreadMaterializeCommand["controlledThreadReservationId"],
        },
        {
          ...command,
          threadId: `${command.threadId}-other` as AgentControlThreadMaterializeCommand["threadId"],
        },
        { ...command, stageKind: "implementation" },
        { ...command, roleId: AgentControlRoleId.make("implementation") },
        { ...command, stageOrdinal: 2 },
        { ...command, attemptOrdinal: 2 },
        {
          ...command,
          binding: { ...command.binding, controlState: "taken-over" },
        },
        {
          ...command,
          binding: {
            ...command.binding,
            taskId: AgentControlTaskId.make("other-task"),
          },
        },
        { ...command, runtimeMode: "full-access" },
        { ...command, interactionMode: "default" },
      ];
      for (const mutated of mutations) {
        const result = yield* Effect.exit(
          decideOrchestrationCommand({
            authority: "agent-control",
            command: mutated,
            readModel: readModel(),
          }),
        );
        assert.strictEqual(result._tag, "Failure");
      }

      const deleted = yield* Effect.exit(
        decideOrchestrationCommand({
          authority: "agent-control",
          command,
          readModel: readModel(NOW),
        }),
      );
      assert.strictEqual(deleted._tag, "Failure");

      const existing = yield* Effect.exit(
        decideOrchestrationCommand({
          authority: "agent-control",
          command,
          readModel: {
            ...readModel(),
            threads: [
              {
                id: command.threadId,
                projectId: command.projectId,
                title: command.title,
                modelSelection: command.modelSelection,
                runtimeMode: command.runtimeMode,
                interactionMode: command.interactionMode,
                branch: command.branch,
                worktreePath: command.worktreePath,
                latestTurn: null,
                createdAt: NOW,
                updatedAt: NOW,
                archivedAt: null,
                deletedAt: null,
                messages: [],
                proposedPlans: [],
                activities: [],
                checkpoints: [],
                session: null,
              },
            ],
          },
        }),
      );
      assert.strictEqual(existing._tag, "Failure");
    }),
  );
});
