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

const canary = " RAW SECRET \r\n\t\u00a0";
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

it("redacts assistant deltas but preserves tool lifecycle diagnostics", () => {
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
  assert.strictEqual(projectProviderRuntimeEventForCanonicalLog(tool), tool);
});
