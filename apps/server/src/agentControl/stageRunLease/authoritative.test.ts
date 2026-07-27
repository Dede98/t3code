import {
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { loadAuthoritativeLeaseHistory } from "./authoritative.ts";
import { deriveAgentControlStageRunLeaseId } from "./identity.ts";
import { projectAgentControlStageRunLeaseEvent } from "./projector.ts";
import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "../stageRun/identity.ts";

const projectId = ProjectId.make("lease-authoritative-project");
const taskId = AgentControlTaskId.make("lease-authoritative-task");
const holderId = AgentControlStageRunLeaseHolderId.make("lease-authoritative-holder");
const sourceIdentityFingerprint = "a".repeat(64);
const makeIdentity = Effect.gen(function* () {
  const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  return {
    leaseId,
    stageRunId,
    attemptId: yield* deriveAgentControlAttemptId(stageRunId, 1),
  };
});
const baseMillis = Date.parse("2026-07-26T10:00:00.000Z");
const timestamp = (offset: number) => DateTime.formatIso(DateTime.makeUnsafe(baseMillis + offset));

const reservedEvent = (
  identity: Effect.Success<typeof makeIdentity>,
  leaseId: AgentControlStageRunLeaseId,
  sequence: number,
): AgentControlStageRunLeaseEvent => {
  const commandId = CommandId.make(`lease-authoritative-command-${leaseId}`);
  return {
    sequence,
    streamVersion: 1,
    eventId: EventId.make(`lease-authoritative-event-${leaseId}`),
    type: "agentControl.stageRunLease.reserved",
    aggregateKind: "stage-run-lease",
    aggregateId: leaseId,
    occurredAt: timestamp(sequence),
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      leaseId,
      projectId,
      taskId,
      stageRunId: identity.stageRunId,
      attemptId: identity.attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      holderId,
      fenceToken: 1,
      acquiredAt: timestamp(sequence),
      renewedAt: timestamp(sequence),
      expiresAt: timestamp(sequence + 60_000),
    },
  };
};

const renewedEvent = (
  identity: Effect.Success<typeof makeIdentity>,
  leaseId: AgentControlStageRunLeaseId,
  streamVersion: number,
  sequence: number,
): AgentControlStageRunLeaseEvent => {
  const commandId = CommandId.make(`lease-authoritative-renew-${leaseId}-${streamVersion}`);
  return {
    sequence,
    streamVersion,
    eventId: EventId.make(`lease-authoritative-renew-event-${leaseId}-${streamVersion}`),
    type: "agentControl.stageRunLease.renewed",
    aggregateKind: "stage-run-lease",
    aggregateId: leaseId,
    occurredAt: timestamp(sequence),
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      leaseId,
      stageRunId: identity.stageRunId,
      attemptId: identity.attemptId,
      holderId,
      fenceToken: 1,
      renewedAt: timestamp(sequence),
      expiresAt: timestamp(sequence + 60_000),
    },
  };
};

const fold = Effect.fn("foldAuthoritativeLeaseTestEvents")(function* (
  events: ReadonlyArray<AgentControlStageRunLeaseEvent>,
) {
  let state: AgentControlStageRunLeaseState | null = null;
  for (const event of events) {
    state = yield* projectAgentControlStageRunLeaseEvent(state, event);
  }
  return state!;
});

const history = (
  events: ReadonlyArray<AgentControlStageRunLeaseEvent>,
  projections: ReadonlyArray<AgentControlStageRunLeaseState>,
) =>
  loadAuthoritativeLeaseHistory(
    {
      readGlobal: (after = 0, limit = 500) =>
        Effect.succeed(events.filter((event) => event.sequence > after).slice(0, limit)),
    },
    {
      listAll: Effect.succeed(projections.map((state) => ({ _tag: "Valid" as const, state }))),
    },
  );

it.effect("rejects every incomplete or competing lease history without latest-wins fallback", () =>
  Effect.gen(function* () {
    const identity = yield* makeIdentity;
    const firstId = identity.leaseId;
    const secondId = AgentControlStageRunLeaseId.make("lease-authoritative-second");
    const firstEvent = reservedEvent(identity, firstId, 1);
    const secondEvent = reservedEvent(identity, secondId, 2);
    const first = yield* fold([firstEvent]);

    assert.lengthOf(yield* history([firstEvent], [first]), 1);
    for (const invalid of [
      history([firstEvent], []),
      history([], [first]),
      history([firstEvent, secondEvent], [first]),
      history([{ ...firstEvent, aggregateId: secondId }, secondEvent], [first]),
    ]) {
      const result = yield* Effect.result(invalid);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "AgentControlProjectionCorruptError");
      }
    }
  }),
);

it.effect("paginates more than 500 lease events and detects a late stream gap", () =>
  Effect.gen(function* () {
    const identity = yield* makeIdentity;
    const leaseId = identity.leaseId;
    const events: Array<AgentControlStageRunLeaseEvent> = [reservedEvent(identity, leaseId, 1)];
    for (let streamVersion = 2; streamVersion <= 501; streamVersion += 1) {
      events.push(renewedEvent(identity, leaseId, streamVersion, streamVersion));
    }
    const projection = yield* fold(events);
    assert.lengthOf(yield* history(events, [projection]), 1);

    const gapped = events.map((event) =>
      event.streamVersion === 501 ? { ...event, streamVersion: 502 } : event,
    );
    const result = yield* Effect.result(history(gapped, [projection]));
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure._tag, "AgentControlProjectionCorruptError");
    }
  }),
);

it.effect("preserves lease SQL failures as infrastructure errors", () =>
  Effect.gen(function* () {
    const failure = { _tag: "AgentControlPersistenceSqlError" } as const;
    const result = yield* Effect.result(
      loadAuthoritativeLeaseHistory(
        { readGlobal: () => Effect.fail(failure as never) },
        { listAll: Effect.die("must not run") },
      ),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure._tag, "AgentControlPersistenceSqlError");
    }
  }),
);
