import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { createThreadEnvironmentAtoms } from "./threadCommands.ts";

describe("createThreadEnvironmentAtoms", () => {
  it("exposes the manual continuation sync RPC command", () => {
    const runtime = Atom.runtime(Layer.empty) as unknown as Atom.AtomRuntime<
      EnvironmentRegistry | Crypto.Crypto,
      never
    >;

    const commands = createThreadEnvironmentAtoms(runtime);

    expect(commands.syncContinuation.label).toBe(
      "environment-data:commands:thread:sync-continuation",
    );
  });
});
