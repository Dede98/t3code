import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlGithubSchedulerStateRepository } from "../Services/AgentControlGithubSchedulerState.ts";
import { layer as SchedulerStateLive } from "./AgentControlGithubSchedulerState.ts";

const EPOCH = "2026-07-23T08:00:00.000Z";
const projectId = ProjectId.make("scheduler-cas");
const testLayer = SchedulerStateLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(NodeServices.layer),
);

const state = (input?: {
  readonly schedulerRevision?: number;
  readonly generation?: number;
  readonly lastGithubEventSequence?: number;
}) => ({
  schemaVersion: 1 as const,
  projectId,
  schedulerRevision: input?.schedulerRevision ?? 1,
  generation: input?.generation ?? 1,
  configFingerprint: "fingerprint",
  pollIntervalSeconds: 15,
  lastGithubEventSequence: input?.lastGithubEventSequence ?? 10,
  activity: "active" as const,
  circuitState: "closed" as const,
  consecutiveFailures: 0,
  lastAttemptAt: null,
  nextAttemptAt: EPOCH,
  cooldownUntil: null,
  reasonCode: null,
  updatedAt: EPOCH,
});

it.effect("enforces scheduler compare-and-swap revisions", () =>
  Effect.gen(function* () {
    const repository = yield* AgentControlGithubSchedulerStateRepository;
    yield* repository.save(state(), 0);

    const conflict = yield* Effect.flip(repository.save(state(), 0));
    assert.deepInclude(conflict, {
      _tag: "AgentControlGithubSchedulerConflictError",
      expectedRevision: 0,
      actualRevision: 1,
    });

    const confirmed = yield* repository.save(
      state({ schedulerRevision: 2, lastGithubEventSequence: 11 }),
      1,
    );
    assert.equal(confirmed.schedulerRevision, 2);
    assert.equal(Option.getOrThrow(yield* repository.get(projectId)).schedulerRevision, 2);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("rejects generation and GitHub cursor regression atomically", () =>
  Effect.gen(function* () {
    const repository = yield* AgentControlGithubSchedulerStateRepository;
    yield* repository.save(
      state({ schedulerRevision: 1, generation: 4, lastGithubEventSequence: 20 }),
      0,
    );

    const generationConflict = yield* Effect.flip(
      repository.save(
        state({ schedulerRevision: 2, generation: 3, lastGithubEventSequence: 21 }),
        1,
      ),
    );
    assert.equal(generationConflict._tag, "AgentControlGithubSchedulerConflictError");

    const cursorConflict = yield* Effect.flip(
      repository.save(
        state({ schedulerRevision: 2, generation: 4, lastGithubEventSequence: 19 }),
        1,
      ),
    );
    assert.equal(cursorConflict._tag, "AgentControlGithubSchedulerConflictError");
    const persisted = Option.getOrThrow(yield* repository.get(projectId));
    assert.equal(persisted.generation, 4);
    assert.equal(persisted.lastGithubEventSequence, 20);
    assert.equal(persisted.schedulerRevision, 1);
  }).pipe(Effect.provide(testLayer)),
);
