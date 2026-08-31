import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AGENT_CONTROL_PROJECT_MODES,
  AgentControlProjectMode,
  AgentControlProjectModeChangedEvent,
  AgentControlProjectState,
  AgentControlRequestedProjectMode,
  AgentControlSetProjectModeInput,
} from "./agentControlRuntime.ts";

const decodeProjectMode = Schema.decodeUnknownEffect(AgentControlProjectMode);
const decodeProjectState = Schema.decodeUnknownEffect(AgentControlProjectState);
const decodeSetProjectModeInput = Schema.decodeUnknownEffect(AgentControlSetProjectModeInput);
const decodeRequestedMode = Schema.decodeUnknownEffect(AgentControlRequestedProjectMode);
const decodeProjectEvent = Schema.decodeUnknownEffect(AgentControlProjectModeChangedEvent);

it.effect("decodes every declared Agent Control project mode", () =>
  Effect.gen(function* () {
    for (const mode of AGENT_CONTROL_PROJECT_MODES) {
      assert.equal(yield* decodeProjectMode(mode), mode);
    }
  }),
);

it.effect("decodes the synthetic manual default state", () =>
  Effect.gen(function* () {
    const state = yield* decodeProjectState({
      schemaVersion: 1,
      projectId: "project-default",
      mode: "manual",
      pausedFromMode: null,
      revision: 0,
      sequence: 0,
      updatedAt: null,
    });
    assert.equal(state.mode, "manual");
    assert.equal(state.revision, 0);
  }),
);

it.effect("keeps command authority out of the client input contract", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeSetProjectModeInput({
      commandId: "command-observe",
      projectId: "project-observe",
      expectedRevision: 0,
      mode: "observe",
      authority: "system",
    });
    assert.deepStrictEqual(decoded, {
      commandId: "command-observe",
      projectId: "project-observe",
      expectedRevision: 0,
      mode: "observe",
    });
    assert.equal(Object.hasOwn(decoded, "authority"), false);
  }),
);

it.effect("decodes Armed only on the historical request boundary", () =>
  Effect.gen(function* () {
    assert.equal(yield* decodeRequestedMode("armed"), "armed");
    assert.equal(
      (yield* Effect.result(
        decodeProjectState({
          schemaVersion: 1,
          projectId: "project-armed",
          mode: "armed",
          pausedFromMode: null,
          revision: 1,
          sequence: 1,
          updatedAt: "2026-08-31T10:00:00.000Z",
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeProjectEvent({
          eventId: "event-armed",
          type: "agentControl.project.mode.changed",
          aggregateKind: "project-controller",
          aggregateId: "project-armed",
          occurredAt: "2026-08-31T10:00:00.000Z",
          commandId: "command-armed",
          causationEventId: null,
          correlationId: "command-armed",
          authority: "human",
          payload: {
            projectId: "project-armed",
            previousMode: "observe",
            mode: "armed",
            previousPausedFromMode: null,
            pausedFromMode: null,
            changedAt: "2026-08-31T10:00:00.000Z",
          },
          metadata: { schemaVersion: 1 },
          streamVersion: 2,
          sequence: 2,
        }),
      ))._tag,
      "Failure",
    );
  }),
);
