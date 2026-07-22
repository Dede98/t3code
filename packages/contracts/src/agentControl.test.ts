import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AGENT_CONTROL_ROLES,
  AgentControlAppPolicy,
  AgentControlProjectPolicy,
  AgentControlRole,
  AgentControlRoleRoute,
} from "./agentControl.ts";

const decodeRole = Schema.decodeUnknownSync(AgentControlRole);
const decodeRoute = Schema.decodeUnknownSync(AgentControlRoleRoute);
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

  it("decodes an app baseline and targeted project overrides independently", () => {
    const appPolicy = decodeAppPolicy({
      providerAllowlist: ["codex-work"],
      roleRoutes: {
        orchestrator: route,
        planner: route,
        implementer: route,
        reviewer: route,
        repair: route,
        verifier: route,
      },
      defaultFallbacks: [{ instanceId: "codex-work", model: "gpt-5.3-codex" }],
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
    expect(projectPolicy.roleRoutes?.reviewer?.driverKind).toBe("claudeAgent");
    expect(projectPolicy.roleRoutes?.implementer).toBeUndefined();
    expect(projectPolicy.providerAllowlist).toBeUndefined();
  });
});
