import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, EventId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlGithubEventStore } from "../Services/AgentControlGithubEventStore.ts";
import { layer as GithubEventStoreLive } from "./AgentControlGithubEventStore.ts";

const EPOCH = "2026-07-23T08:00:00.000Z";
const repository = {
  repositoryNodeId: "repository-node",
  nameWithOwner: "owner/repo",
} as const;
const settings = {
  trackerKind: "github" as const,
  readyLabel: "agent:ready",
  pausedLabel: "agent:paused",
  trustedLogins: ["trusted"],
  pollIntervalSeconds: 15,
};
const testLayer = GithubEventStoreLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(NodeServices.layer),
);

const configDraft = (projectId: ProjectId, suffix: string) => {
  const commandId = CommandId.make(`config-${suffix}`);
  return {
    eventId: EventId.make(`config-${suffix}`),
    type: "agentControl.github.config.set" as const,
    aggregateKind: "github-intake" as const,
    aggregateId: projectId,
    occurredAt: EPOCH,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "human" as const,
    payload: { projectId, settings, repository, configuredAt: EPOCH },
    metadata: { schemaVersion: 1 as const },
  };
};

const failureDraft = (projectId: ProjectId, suffix: string) => {
  const commandId = CommandId.make(`failure-${suffix}`);
  return {
    eventId: EventId.make(`failure-${suffix}`),
    type: "agentControl.github.poll.failed" as const,
    aggregateKind: "github-intake" as const,
    aggregateId: projectId,
    occurredAt: EPOCH,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller" as const,
    payload: {
      projectId,
      attemptedAt: EPOCH,
      completedAt: EPOCH,
      errorCode: "github-timeout" as const,
      invalidateCursor: false,
    },
    metadata: { schemaVersion: 1 as const },
  };
};

it.effect("reads one project by global sequence across interleaved streams", () =>
  Effect.gen(function* () {
    const store = yield* AgentControlGithubEventStore;
    const first = ProjectId.make("eventstore-first");
    const second = ProjectId.make("eventstore-second");
    const firstConfig = yield* store.append({
      projectId: first,
      expectedStreamVersion: 0,
      events: [configDraft(first, "first")],
    });
    yield* store.append({
      projectId: second,
      expectedStreamVersion: 0,
      events: [configDraft(second, "second")],
    });
    const firstFailures = yield* store.append({
      projectId: first,
      expectedStreamVersion: 1,
      events: [failureDraft(first, "first-1"), failureDraft(first, "first-2")],
    });

    const page = yield* store.readProjectAfterSequence(first, firstConfig[0]!.sequence, 1);
    assert.deepStrictEqual(
      page.map((event) => event.sequence),
      [firstFailures[0]!.sequence],
    );
    assert.isAbove(page[0]!.sequence, firstConfig[0]!.sequence + 1);
    assert.equal(page[0]!.streamVersion, 2);

    const tail = yield* store.readProjectAfterSequence(first, page[0]!.sequence, 10);
    assert.deepStrictEqual(
      tail.map((event) => event.streamVersion),
      [3],
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("strictly rejects corrupt project replay rows", () =>
  Effect.gen(function* () {
    const store = yield* AgentControlGithubEventStore;
    const sql = yield* SqlClient.SqlClient;
    const projectId = ProjectId.make("eventstore-corrupt");
    yield* store.append({
      projectId,
      expectedStreamVersion: 0,
      events: [configDraft(projectId, "corrupt")],
    });
    yield* sql`
      UPDATE agent_control_events
      SET payload_json = '{"projectId":1}'
      WHERE stream_id = ${projectId}
    `;

    const result = yield* Effect.result(store.readProjectAfterSequence(projectId, 0, 10));
    assert.equal(result._tag, "Failure");
  }).pipe(Effect.provide(testLayer)),
);
