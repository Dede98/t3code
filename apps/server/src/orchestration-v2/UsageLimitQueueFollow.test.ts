import { assert, it } from "@effect/vitest";
import {
  NodeId,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  RunId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { movedPastUsageLimit } from "./UsageLimitQueueFollow.ts";

const NOW = Date.parse("2026-10-28T12:00:00.000Z");
const at = (minutes: number) => DateTime.makeUnsafe(NOW + minutes * 60_000);
const limitedAccount = ProviderInstanceId.make("account-a");
const otherAccount = ProviderInstanceId.make("account-b");

const run = (
  id: string,
  providerInstanceId: ProviderInstanceId,
  ordinal: number,
  status: OrchestrationV2Run["status"],
  times: { readonly requested: number; readonly started?: number; readonly completed?: number },
) =>
  ({
    id: RunId.make(id),
    providerInstanceId,
    ordinal,
    status,
    rootNodeId: NodeId.make(`${id}:root`),
    requestedAt: at(times.requested),
    startedAt: times.started === undefined ? null : at(times.started),
    completedAt: times.completed === undefined ? null : at(times.completed),
  }) as unknown as OrchestrationV2Run;

const limited = run("limited", limitedAccount, 1, "failed", {
  requested: 0,
  started: 0,
  completed: 1,
});
const interrupted = run("limited", limitedAccount, 1, "interrupted", {
  requested: 0,
  started: 0,
  completed: 1,
});
const continuation = run("continuation", otherAccount, 3, "completed", {
  requested: 2,
  started: 2,
  completed: 10,
});
const firstFollower = run("first-follower", otherAccount, 4, "running", {
  requested: 0.5,
  started: 10,
});
const limitError = {
  id: TurnItemId.make("limit-error"),
  type: "error",
  status: "failed",
  runId: limited.id,
  nodeId: limited.rootNodeId,
  ordinal: 1,
  updatedAt: at(1),
  failure: { class: "usage_limit", message: "Limit reached", code: null, retryable: null },
};

const projection = (runs: ReadonlyArray<OrchestrationV2Run>) =>
  ({
    thread: { providerInstanceId: otherAccount },
    runs,
    turnItems: [limitError],
  }) as unknown as Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "turnItems">;

it.each([
  {
    name: "a message queued during the stop follows the continuation",
    runs: [limited, continuation],
    queued: { account: limitedAccount, requested: 0.5 },
    expected: true,
  },
  {
    name: "the next queued message follows after the first one moved",
    runs: [limited, continuation, firstFollower],
    queued: { account: limitedAccount, requested: 0.6 },
    expected: true,
  },
  {
    name: "a message pinned to the limited account after the move keeps it",
    runs: [limited, continuation],
    queued: { account: limitedAccount, requested: 5 },
    expected: false,
  },
  {
    name: "a plain account change without a continuation keeps it",
    runs: [limited],
    queued: { account: limitedAccount, requested: 0.5 },
    expected: false,
  },
  {
    name: "an interrupted account keeps its queue",
    runs: [interrupted, continuation],
    queued: { account: limitedAccount, requested: 0.5 },
    expected: false,
  },
  {
    name: "a message already on the thread's account stays",
    runs: [limited, continuation],
    queued: { account: otherAccount, requested: 0.5 },
    expected: false,
  },
])("$name", ({ runs, queued, expected }) => {
  assert.equal(
    movedPastUsageLimit(projection(runs), {
      providerInstanceId: queued.account,
      requestedAt: at(queued.requested),
    }),
    expected,
  );
});
