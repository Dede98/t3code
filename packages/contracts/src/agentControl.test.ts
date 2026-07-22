import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AGENT_CONTROL_ROLES,
  AgentControlAppPolicy,
  AgentControlPolicyDefaults,
  AgentControlProjectPolicy,
  AgentControlRole,
  AgentControlRoleRoute,
} from "./agentControl.ts";

const decodeRole = Schema.decodeUnknownSync(AgentControlRole);
const decodeRoute = Schema.decodeUnknownSync(AgentControlRoleRoute);
const decodeDefaults = Schema.decodeUnknownSync(AgentControlPolicyDefaults);
const decodeAppPolicy = Schema.decodeUnknownSync(AgentControlAppPolicy);
const decodeProjectPolicy = Schema.decodeUnknownSync(AgentControlProjectPolicy);

const route = {
  candidates: [{ instanceId: "codex-work", model: "gpt-5.4" }],
  strict: true,
};

describe("Agent Control policy contracts", () => {
  it("defines the six Agent Control roles", () => {
    expect(AGENT_CONTROL_ROLES.map((role) => decodeRole(role))).toEqual([
      "orchestrator",
      "planner",
      "implementer",
      "reviewer",
      "repair",
      "verifier",
    ]);
    expect(() => decodeRole("automation")).toThrow();
  });

  it("requires ordered route candidates and an explicit strictness flag", () => {
    expect(decodeRoute(route)).toEqual(route);
    expect(() => decodeRoute({ candidates: route.candidates })).toThrow();
    expect(() => decodeRoute({ candidates: [], strict: true })).toThrow();
  });

  it("requires non-empty built-in defaults", () => {
    expect(
      decodeDefaults({
        defaultFallbacks: [{ instanceId: "codex-work", model: "gpt-5.4" }],
      }),
    ).toEqual({
      defaultFallbacks: [{ instanceId: "codex-work", model: "gpt-5.4" }],
    });
    expect(() => decodeDefaults({ defaultFallbacks: [] })).toThrow();
  });

  it("decodes app and project policies as fully optional overrides", () => {
    expect(decodeAppPolicy({})).toEqual({});
    expect(decodeProjectPolicy({})).toEqual({});

    const appPolicy = decodeAppPolicy({
      providerAllowlist: ["codex-work"],
      roleRoutes: {
        reviewer: route,
      },
    });
    const projectPolicy = decodeProjectPolicy({
      roleRoutes: {
        reviewer: {
          candidates: [{ instanceId: "claude-work", model: "claude-sonnet-5" }],
          driverKind: "claudeAgent",
          strict: false,
        },
      },
      fullAccess: true,
    });

    expect(appPolicy.providerAllowlist).toEqual(["codex-work"]);
    expect(appPolicy.roleRoutes?.reviewer).toEqual(route);
    expect(appPolicy.roleRoutes?.implementer).toBeUndefined();
    expect(projectPolicy.roleRoutes?.reviewer?.driverKind).toBe("claudeAgent");
    expect(projectPolicy.roleRoutes?.implementer).toBeUndefined();
    expect(projectPolicy.providerAllowlist).toBeUndefined();
  });
});
