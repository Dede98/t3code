import { CommandId, EventId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  createDefaultAgentControlProjectState,
  decideAgentControlProjectCommand,
} from "./decider.ts";
import { projectAgentControlEvent } from "./projector.ts";

const projectId = ProjectId.make("project-decider");
const now = "2026-07-22T12:00:00.000Z";

const command = (mode: "manual" | "observe" | "run-once" | "armed" | "paused") => ({
  type: "agentControl.project.mode.set" as const,
  commandId: CommandId.make(`command-${mode}`),
  projectId,
  expectedRevision: 0,
  mode,
});

const decide = (
  state: ReturnType<typeof createDefaultAgentControlProjectState>,
  mode: Parameters<typeof command>[0],
) =>
  decideAgentControlProjectCommand({
    state,
    command: { ...command(mode), expectedRevision: state.revision },
    eventId: EventId.make(`event-${state.mode}-${mode}`),
    occurredAt: now,
    authority: "human",
  });

it.effect("uses Manual as the safe default and treats the active target as a no-op", () =>
  Effect.gen(function* () {
    const state = createDefaultAgentControlProjectState(projectId);
    assert.deepStrictEqual(state, {
      schemaVersion: 1,
      projectId,
      mode: "manual",
      pausedFromMode: null,
      revision: 0,
      sequence: 0,
      updatedAt: null,
    });
    assert.deepStrictEqual(yield* decide(state, "manual"), []);
  }),
);

it.effect("allows every V1 transition and maintains pause origin", () =>
  Effect.gen(function* () {
    const allowed = [
      ["manual", null, "observe", null],
      ["observe", null, "manual", null],
      ["observe", null, "paused", "observe"],
      ["observe", null, "run-once", null],
      ["run-once", null, "paused", "run-once"],
      ["paused", "run-once", "run-once", null],
      ["run-once", null, "manual", null],
      ["paused", "observe", "observe", null],
      ["paused", "observe", "manual", null],
    ] as const;
    for (const [fromMode, fromPause, toMode, toPause] of allowed) {
      const state = {
        ...createDefaultAgentControlProjectState(projectId),
        mode: fromMode,
        pausedFromMode: fromPause,
        revision: fromMode === "manual" ? 0 : 1,
        sequence: fromMode === "manual" ? 0 : 1,
        updatedAt: fromMode === "manual" ? null : now,
      };
      const events = yield* decide(state, toMode);
      assert.equal(events.length, 1);
      const event = {
        ...events[0]!,
        streamVersion: state.revision + 1,
        sequence: state.sequence + 1,
      };
      const projected = yield* projectAgentControlEvent(state, event);
      assert.equal(projected.mode, toMode);
      assert.equal(projected.pausedFromMode, toPause);
    }
  }),
);

it.effect("rejects unavailable and semantically invalid transitions", () =>
  Effect.gen(function* () {
    const manual = createDefaultAgentControlProjectState(projectId);
    const manualPaused = yield* Effect.result(decide(manual, "paused"));
    assert.equal(manualPaused._tag, "Failure");
    if (manualPaused._tag === "Failure")
      assert.equal(manualPaused.failure.code, "transition-not-allowed");

    const runOnce = yield* Effect.result(decide(manual, "run-once"));
    assert.equal(runOnce._tag, "Failure");
    if (runOnce._tag === "Failure") assert.equal(runOnce.failure.code, "transition-not-allowed");

    const armed = yield* Effect.result(decide(manual, "armed"));
    assert.equal(armed._tag, "Failure");
    if (armed._tag === "Failure") assert.equal(armed.failure.code, "mode-not-available");
  }),
);

it.effect("reserves run-once reset for system authority and takeover for humans", () =>
  Effect.gen(function* () {
    const observe = yield* decide(createDefaultAgentControlProjectState(projectId), "observe");
    const observeState = yield* projectAgentControlEvent(
      createDefaultAgentControlProjectState(projectId),
      { ...observe[0]!, streamVersion: 1, sequence: 1 },
    );
    const activation = yield* decide(observeState, "run-once");
    const active = yield* projectAgentControlEvent(observeState, {
      ...activation[0]!,
      streamVersion: 2,
      sequence: 2,
    });
    const systemReset = yield* decideAgentControlProjectCommand({
      state: active,
      command: { ...command("observe"), expectedRevision: active.revision },
      eventId: EventId.make("event-system-reset"),
      occurredAt: now,
      authority: "system",
    });
    assert.equal(systemReset[0]?.payload.mode, "observe");

    const humanReset = yield* Effect.result(decide(active, "observe"));
    assert.equal(humanReset._tag, "Failure");
    if (humanReset._tag === "Failure") {
      assert.equal(humanReset.failure.code, "transition-not-allowed");
    }
    const staleSystemNoop = yield* Effect.result(
      decideAgentControlProjectCommand({
        state: observeState,
        command: { ...command("observe"), expectedRevision: observeState.revision },
        eventId: EventId.make("event-stale-system-reset"),
        occurredAt: now,
        authority: "system",
      }),
    );
    assert.equal(staleSystemNoop._tag, "Failure");
    if (staleSystemNoop._tag === "Failure") {
      assert.equal(staleSystemNoop.failure.code, "transition-not-allowed");
    }
    assert.equal((yield* decide(active, "manual"))[0]?.payload.mode, "manual");
  }),
);
