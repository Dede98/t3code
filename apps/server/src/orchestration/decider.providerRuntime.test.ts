import {
  CommandId,
  EventId,
  MessageId,
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

  it.effect("binds a Verification result seal to the matching completed lifecycle", () =>
    Effect.gen(function* () {
      const providerTurnId = TurnId.make("verification-provider-turn");
      const lifecycle = {
        runtimeEventId: EventId.make("verification-runtime-terminal"),
        runtimeEventType: "turn.completed" as const,
        providerInstanceId,
        providerTurnId,
        providerState: "completed" as const,
      };
      const seal = {
        schemaVersion: 1 as const,
        handoffId: "verification-handoff",
        providerDeliveryId: "verification-delivery",
        providerInstanceId,
        providerTurnId,
        resultSchemaFingerprint: "f".repeat(64),
        sourceDisposition: "captured" as const,
        finalMessageId: MessageId.make("verification-final-message"),
        sourceEventId: EventId.make("verification-source-event"),
        outputDigest: "a".repeat(64),
        outputByteLength: 42,
      };
      const result = yield* decideOrchestrationCommand({
        authority: "system",
        readModel,
        command: {
          type: "thread.session.set",
          commandId: CommandId.make("verification-terminal-command"),
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
          providerRuntimeLifecycle: lifecycle,
          verificationResultSource: seal,
          createdAt: now,
        },
      });
      const event = Array.isArray(result) ? result[0]! : result;
      assert.deepStrictEqual(event.metadata, {
        providerRuntimeLifecycle: lifecycle,
        verificationResultSource: seal,
      });

      const mismatch = yield* Effect.flip(
        decideOrchestrationCommand({
          authority: "system",
          readModel,
          command: {
            type: "thread.session.set",
            commandId: CommandId.make("verification-terminal-mismatch-command"),
            threadId,
            session: event.payload.session,
            providerRuntimeLifecycle: { ...lifecycle, providerState: "failed" },
            verificationResultSource: seal,
            createdAt: now,
          },
        }),
      );
      assert.equal(mismatch._tag, "OrchestrationCommandInvariantError");
    }),
  );

  it.effect("copies typed Assistant correlation and rejects a cross-turn command", () =>
    Effect.gen(function* () {
      const providerTurnId = TurnId.make("verification-provider-turn");
      const correlation = {
        runtimeEventId: EventId.make("verification-runtime-delta"),
        runtimeEventType: "content.delta" as const,
        providerInstanceId,
        providerTurnId,
      };
      const command = {
        type: "thread.message.assistant.delta" as const,
        commandId: CommandId.make("verification-assistant-delta-command"),
        threadId,
        messageId: MessageId.make("verification-assistant-message"),
        delta: "result bytes",
        turnId: providerTurnId,
        providerRuntimeMessage: correlation,
        createdAt: now,
      };
      const result = yield* decideOrchestrationCommand({
        authority: "system",
        readModel,
        command,
      });
      const event = Array.isArray(result) ? result[0]! : result;
      assert.deepStrictEqual(event.metadata, { providerRuntimeMessage: correlation });
      const mismatch = yield* Effect.flip(
        decideOrchestrationCommand({
          authority: "system",
          readModel,
          command: { ...command, turnId: TurnId.make("foreign-turn") },
        }),
      );
      assert.equal(mismatch._tag, "OrchestrationCommandInvariantError");
    }),
  );

  it.effect("creates a non-message Verification capture only for matching authority", () =>
    Effect.gen(function* () {
      const providerTurnId = TurnId.make("verification-provider-turn");
      const runtime = {
        runtimeEventId: EventId.make("verification-runtime-capture"),
        runtimeEventType: "content.delta" as const,
        providerInstanceId,
        providerTurnId,
      };
      const capture = {
        schemaVersion: 1 as const,
        disposition: "authority" as const,
        handoffId: "verification-handoff",
        providerDeliveryId: "verification-delivery",
        providerInstanceId,
        providerTurnId,
        resultSchemaFingerprint: "f".repeat(64),
      };
      const command = {
        type: "thread.verification-result.capture" as const,
        commandId: CommandId.make("provider:verification-runtime-capture:verification-result"),
        threadId,
        messageId: MessageId.make("verification-message"),
        turnId: providerTurnId,
        fragment: {
          kind: "delta" as const,
          text: "result bytes",
          byteLength: 12,
          cumulativeByteLength: 12,
        },
        providerRuntimeMessage: runtime,
        verificationResultCapture: capture,
        createdAt: now,
      };
      const result = yield* decideOrchestrationCommand({
        authority: "system",
        readModel,
        command,
      });
      const event = Array.isArray(result) ? result[0]! : result;
      assert.equal(event.type, "thread.verification-result-fragment-captured");
      assert.deepStrictEqual(event.metadata, {
        providerRuntimeMessage: runtime,
        verificationResultCapture: capture,
      });
      const mismatch = yield* Effect.flip(
        decideOrchestrationCommand({
          authority: "system",
          readModel,
          command: {
            ...command,
            verificationResultCapture: {
              ...capture,
              providerInstanceId: ProviderInstanceId.make("foreign-provider"),
            },
          },
        }),
      );
      assert.equal(mismatch._tag, "OrchestrationCommandInvariantError");
    }),
  );
});
