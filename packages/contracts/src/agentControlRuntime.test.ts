import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AGENT_CONTROL_PROJECT_MODES,
  AgentControlProjectMode,
  AgentControlProjectState,
  AgentControlSetProjectModeInput,
} from "./agentControlRuntime.ts";

const decodeProjectMode = Schema.decodeUnknownEffect(AgentControlProjectMode);
const decodeProjectState = Schema.decodeUnknownEffect(AgentControlProjectState);
const decodeSetProjectModeInput = Schema.decodeUnknownEffect(AgentControlSetProjectModeInput);

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
