import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";

import { projectProviderRuntimeEventForCanonicalLog } from "./ProviderRuntimeEventLogProjection.ts";

const canary = '  T3_CANARY_\t\r\n\u00a0\u2028\u2029多字_"\\  ';
const escapedCanary = JSON.stringify(canary).slice(1, -1);

const assistantCompletion = (provider: string): ProviderRuntimeEvent => ({
  eventId: EventId.make(`event-${provider}`),
  provider: ProviderDriverKind.make(provider),
  providerInstanceId: ProviderInstanceId.make(provider),
  threadId: ThreadId.make(`thread-${provider}`),
  turnId: TurnId.make(`turn-${provider}`),
  itemId: RuntimeItemId.make(`item-${provider}`),
  createdAt: "2026-08-28T10:00:00.000Z",
  type: "item.completed",
  payload: {
    itemType: "assistant_message",
    status: "completed",
    title: canary,
    detail: canary,
    authorityDetail: canary,
    data: { nested: { text: canary } },
  },
  providerRefs: {
    providerTurnId: `turn-${provider}`,
    providerItemId: ProviderItemId.make(`item-${provider}`),
  },
  raw: {
    source: "codex.app-server.notification",
    method: "item/completed",
    payload: { nested: { text: canary } },
  },
});

it("redacts every generic provider assistant completion without mutating authority", () => {
  for (const provider of ["codex", "cursor", "grok", "claudeAgent", "opencode"]) {
    const event = assistantCompletion(provider);
    const before = structuredClone(event);
    const projection = projectProviderRuntimeEventForCanonicalLog(event);
    const serialized = JSON.stringify(projection);

    assert.deepStrictEqual(event, before, provider);
    assert.notInclude(serialized, canary, provider);
    assert.notInclude(serialized, escapedCanary, provider);
    assert.notInclude(serialized, "authorityDetail", provider);
    assert.notInclude(serialized, '"detail"', provider);
    assert.notInclude(serialized, '"data"', provider);
    assert.notInclude(serialized, '"raw"', provider);
    assert.deepStrictEqual(projection, {
      eventId: `event-${provider}`,
      provider,
      providerInstanceId: provider,
      threadId: `thread-${provider}`,
      createdAt: "2026-08-28T10:00:00.000Z",
      turnId: `turn-${provider}`,
      itemId: `item-${provider}`,
      providerRefs: {
        providerTurnId: `turn-${provider}`,
        providerItemId: `item-${provider}`,
      },
      type: "item.completed",
      payload: { itemType: "assistant_message", status: "completed" },
    });
  }
});

it("redacts assistant deltas and keeps only closed tool lifecycle diagnostics", () => {
  const delta: ProviderRuntimeEvent = {
    eventId: EventId.make("event-assistant-delta"),
    provider: ProviderDriverKind.make("cursor"),
    providerInstanceId: ProviderInstanceId.make("cursor"),
    threadId: ThreadId.make("thread-assistant-delta"),
    turnId: TurnId.make("turn-assistant-delta"),
    itemId: RuntimeItemId.make("item-assistant-delta"),
    createdAt: "2026-08-28T10:00:00.000Z",
    type: "content.delta",
    payload: { streamKind: "assistant_text", delta: canary, contentIndex: 2 },
    raw: {
      source: "acp.jsonrpc",
      method: "session/update",
      payload: { text: canary },
    },
  };
  const deltaBefore = structuredClone(delta);
  const deltaProjection = projectProviderRuntimeEventForCanonicalLog(delta);
  assert.deepStrictEqual(delta, deltaBefore);
  assert.deepStrictEqual(deltaProjection, {
    eventId: "event-assistant-delta",
    provider: "cursor",
    providerInstanceId: "cursor",
    threadId: "thread-assistant-delta",
    createdAt: "2026-08-28T10:00:00.000Z",
    turnId: "turn-assistant-delta",
    itemId: "item-assistant-delta",
    type: "content.delta",
    payload: { streamKind: "assistant_text", contentIndex: 2 },
  });

  const tool: ProviderRuntimeEvent = {
    eventId: EventId.make("event-tool"),
    provider: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    threadId: ThreadId.make("thread-tool"),
    turnId: TurnId.make("turn-tool"),
    itemId: RuntimeItemId.make("item-tool"),
    createdAt: "2026-08-28T10:00:00.000Z",
    type: "item.completed",
    payload: {
      itemType: "command_execution",
      status: "completed",
      title: "Command run",
      detail: "git status",
      data: { exitCode: 0 },
    },
  };
  assert.deepStrictEqual(projectProviderRuntimeEventForCanonicalLog(tool), {
    eventId: "event-tool",
    provider: "codex",
    providerInstanceId: "codex",
    threadId: "thread-tool",
    createdAt: "2026-08-28T10:00:00.000Z",
    turnId: "turn-tool",
    itemId: "item-tool",
    type: "item.completed",
    payload: {
      itemType: "command_execution",
      status: "completed",
      exitCode: 0,
    },
  });
});

it("drops the canary from every free-form, nested, array, and unknown event path", () => {
  const events = [
    {
      ...assistantCompletion("codex"),
      unknownTop: canary,
      unknownNested: { values: [canary, { deeper: canary }] },
    },
    {
      eventId: EventId.make("event-command-canary"),
      provider: ProviderDriverKind.make("cursor"),
      providerInstanceId: ProviderInstanceId.make("cursor"),
      threadId: ThreadId.make("thread-command-canary"),
      turnId: TurnId.make("turn-command-canary"),
      itemId: RuntimeItemId.make("item-command-canary"),
      createdAt: "2026-08-28T10:00:00.000Z",
      type: "item.completed",
      payload: {
        itemType: "command_execution",
        status: "failed",
        title: canary,
        detail: canary,
        authorityDetail: canary,
        data: { command: canary, output: [canary, { nested: canary }], exitCode: 17 },
      },
      raw: { command: canary, output: canary },
    },
    {
      eventId: EventId.make("event-hook-canary"),
      provider: ProviderDriverKind.make("claudeAgent"),
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      threadId: ThreadId.make("thread-hook-canary"),
      createdAt: "2026-08-28T10:00:00.000Z",
      type: "hook.completed",
      payload: {
        hookId: "hook-safe-id",
        outcome: "error",
        output: canary,
        stdout: canary,
        stderr: canary,
        exitCode: 23,
      },
    },
    {
      eventId: EventId.make("event-delta-canary"),
      provider: ProviderDriverKind.make("opencode"),
      providerInstanceId: ProviderInstanceId.make("opencode"),
      threadId: ThreadId.make("thread-delta-canary"),
      createdAt: "2026-08-28T10:00:00.000Z",
      type: "content.delta",
      payload: { streamKind: "assistant_text", delta: canary, contentIndex: 4, summaryIndex: 2 },
      raw: { array: [canary] },
    },
    {
      eventId: "event-foreign-canary",
      provider: "foreign-provider",
      threadId: "thread-foreign-canary",
      createdAt: "2026-08-28T10:00:00.000Z",
      type: "future.foreign-event",
      payload: { detail: canary, data: [{ raw: canary }] },
      raw: { title: canary },
      unknownTop: canary,
    },
  ] as unknown as ReadonlyArray<ProviderRuntimeEvent>;

  for (const event of events) {
    const before = structuredClone(event);
    const projected = projectProviderRuntimeEventForCanonicalLog(event);
    const serialized = JSON.stringify(projected) ?? "";
    assert.deepStrictEqual(event, before);
    assert.notInclude(serialized, canary);
    assert.notInclude(serialized, escapedCanary);
    for (const forbiddenKey of [
      "authorityDetail",
      "detail",
      "data",
      "raw",
      "title",
      "delta",
      "command",
      "output",
      "stdout",
      "stderr",
      "unknownTop",
      "unknownNested",
    ]) {
      assert.notInclude(serialized, `"${forbiddenKey}"`);
    }
    if (String(event.type) === "future.foreign-event") assert.isUndefined(projected);
  }
});

it("retains only expressly classified identities, enums, numbers, and booleans", () => {
  const usage: ProviderRuntimeEvent = {
    eventId: EventId.make("event-usage-safe"),
    provider: ProviderDriverKind.make("grok"),
    providerInstanceId: ProviderInstanceId.make("grok"),
    threadId: ThreadId.make("thread-usage-safe"),
    createdAt: "2026-08-28T10:00:00.000Z",
    type: "thread.token-usage.updated",
    payload: {
      usage: {
        usedTokens: 100,
        inputTokens: 70,
        outputTokens: 30,
        toolUses: 2,
        durationMs: 400,
        compactsAutomatically: true,
      },
    },
  };
  assert.deepStrictEqual(projectProviderRuntimeEventForCanonicalLog(usage), {
    eventId: "event-usage-safe",
    provider: "grok",
    providerInstanceId: "grok",
    threadId: "thread-usage-safe",
    createdAt: "2026-08-28T10:00:00.000Z",
    type: "thread.token-usage.updated",
    payload: {
      usage: {
        usedTokens: 100,
        inputTokens: 70,
        outputTokens: 30,
        toolUses: 2,
        durationMs: 400,
        compactsAutomatically: true,
      },
    },
  });
});
