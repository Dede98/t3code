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

    for (const mode of ["run-once", "armed"] as const) {
      const result = yield* Effect.result(decide(manual, mode));
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.code, "mode-not-available");
    }
  }),
);
