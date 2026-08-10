import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideOrchestrationCommand } from "./decider.ts";

const now = "2026-08-10T10:11:12.345Z";
const projectId = ProjectId.make("project-provider-runtime");
const threadId = ThreadId.make("thread-provider-runtime");
const providerInstanceId = ProviderInstanceId.make("codex-main");

const readModel: OrchestrationReadModel = {
  snapshotSequence: 1,
  projects: [
    {
      id: projectId,
      title: "Provider runtime",
      workspaceRoot: "/tmp/provider-runtime",
      defaultModelSelection: null,
      scripts: [],
      createdAt: now,
      updatedAt: now,
      deletedAt: null,
    },
  ],
  threads: [
    {
      id: threadId,
      projectId,
      title: "Provider runtime",
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5.6" },
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: "feat/provider-runtime",
      worktreePath: "/tmp/provider-runtime",
      latestTurn: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      messages: [],
      proposedPlans: [],
      activities: [],
      checkpoints: [],
      session: null,
    },
  ],
  updatedAt: now,
};

it.layer(NodeServices.layer)("provider runtime session metadata", (it) => {
  it.effect("copies only the typed lifecycle structure into event metadata", () =>
    Effect.gen(function* () {
      const providerTurnId = TurnId.make("provider-turn-1");
      const providerRuntimeLifecycle = {
        runtimeEventId: EventId.make("runtime-event-1"),
        runtimeEventType: "turn.completed",
        providerInstanceId,
        providerTurnId,
        providerState: "cancelled",
        errorMessage: "must not cross the decider boundary",
      } as const;
      const result = yield* decideOrchestrationCommand({
        authority: "system",
        readModel,
        command: {
          type: "thread.session.set",
          commandId: CommandId.make("session-command-1"),
          threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "codex",
            providerInstanceId,
            runtimeMode: "approval-required",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
          providerRuntimeLifecycle,
          createdAt: now,
        },
      });
      const event = Array.isArray(result) ? result[0]! : result;
      assert.deepStrictEqual(event.metadata, {
        providerRuntimeLifecycle: {
          runtimeEventId: "runtime-event-1",
          runtimeEventType: "turn.completed",
          providerInstanceId: "codex-main",
          providerTurnId: "provider-turn-1",
          providerState: "cancelled",
        },
      });
    }),
  );
});
