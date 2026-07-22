import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AGENT_CONTROL_RPC_METHODS,
  AGENT_CONTROL_ROLES,
  AgentControlAppPolicy,
  AgentControlPolicyStateResult,
  AgentControlPolicyDefaults,
  AgentControlPreflightPolicyInput,
  AgentControlPreflightPolicyResult,
  AgentControlPreflightRuntimeInput,
  AgentControlPreflightRuntimeResult,
  AgentControlProjectPolicy,
  AgentControlRole,
  AgentControlRoleRoute,
} from "./agentControl.ts";
import { WsRpcGroup } from "./rpc.ts";

const decodeRole = Schema.decodeUnknownSync(AgentControlRole);
const decodeRoute = Schema.decodeUnknownSync(AgentControlRoleRoute);
const decodeDefaults = Schema.decodeUnknownSync(AgentControlPolicyDefaults);
const decodeAppPolicy = Schema.decodeUnknownSync(AgentControlAppPolicy);
const decodeProjectPolicy = Schema.decodeUnknownSync(AgentControlProjectPolicy);
const decodePreflightInput = Schema.decodeUnknownSync(AgentControlPreflightPolicyInput);
const decodePreflightResult = Schema.decodeUnknownSync(AgentControlPreflightPolicyResult);
const decodeRuntimeInput = Schema.decodeUnknownSync(AgentControlPreflightRuntimeInput);
const decodeRuntimeResult = Schema.decodeUnknownSync(AgentControlPreflightRuntimeResult);
const decodePolicyState = Schema.decodeUnknownSync(AgentControlPolicyStateResult);

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

  it("defines only two-segment Agent Control RPC names and registers them in the RPC group", () => {
    const methods = Object.values(AGENT_CONTROL_RPC_METHODS);
    expect(methods).toEqual([
      "agentControl.getPolicy",
      "agentControl.setProjectPolicy",
      "agentControl.clearProjectPolicy",
      "agentControl.preflightPolicy",
      "agentControl.preflightRuntime",
    ]);
    for (const method of methods) {
      expect(method.split(".")).toHaveLength(2);
      expect(method.startsWith("automation.")).toBe(false);
      expect(WsRpcGroup.requests.has(method)).toBe(true);
    }
  });

  it("preserves omitted, null, and object preflight draft states", () => {
    expect(decodePreflightInput({ projectId: "project-a" })).toEqual({
      projectId: "project-a",
    });
    expect(
      decodePreflightInput({
        projectId: "project-a",
        appPolicy: null,
        projectPolicy: null,
      }),
    ).toEqual({ projectId: "project-a", appPolicy: null, projectPolicy: null });
    expect(
      decodePreflightInput({
        projectId: "project-a",
        appPolicy: { defaultFallbacks: [] },
        projectPolicy: { fullAccess: true },
      }),
    ).toEqual({
      projectId: "project-a",
      appPolicy: { defaultFallbacks: [] },
      projectPolicy: { fullAccess: true },
    });
    expect(
      decodeRuntimeInput({
        projectId: "project-a",
        appPolicy: null,
        projectPolicy: { fullAccess: true },
      }),
    ).toEqual({
      projectId: "project-a",
      appPolicy: null,
      projectPolicy: { fullAccess: true },
    });
  });

  it("decodes closed runtime readiness results", () => {
    const result = decodeRuntimeResult({
      ok: false,
      staticPreflight: { ok: true, roles: [] },
      roles: [
        {
          role: "reviewer",
          accessMode: "restricted",
          strict: false,
          candidates: [
            {
              candidateIndex: 0,
              source: "role-route",
              providerInstanceId: "codex-work",
              model: "gpt-5.4",
              driverKind: "codex",
              providerStatus: "warning",
              authStatus: "unknown",
              checkedAt: "2026-07-22T12:00:00.000Z",
              runtimeReady: false,
              errorCode: "provider-not-ready",
            },
          ],
          selectedCandidateIndex: null,
          errorCode: "role-runtime-unresolved",
        },
      ],
    });

    expect(result.roles[0]?.candidates[0]?.errorCode).toBe("provider-not-ready");
    expect(() =>
      decodeRuntimeResult({
        ...result,
        roles: [
          {
            ...result.roles[0],
            candidates: [
              {
                ...result.roles[0]?.candidates[0],
                errorCode: "provider-leaked-stderr",
              },
            ],
          },
        ],
      }),
    ).toThrow();
  });

  it("decodes array-based wire policy and semantic preflight results", () => {
    const preflight = decodePreflightResult({
      ok: false,
      roles: [
        {
          role: "reviewer",
          accessMode: "restricted",
          strict: true,
          validCandidates: [],
        },
      ],
      errors: [
        {
          code: "driver-kind-mismatch",
          role: "reviewer",
          source: "role-route",
          candidateIndex: 0,
          instanceId: "codex-work",
          expectedDriverKind: "claudeAgent",
          actualDriverKind: "codex",
        },
      ],
    });
    expect(preflight.ok).toBe(false);
    expect(
      decodePolicyState({
        appPolicy: null,
        projectPolicy: null,
        preflight,
      }),
    ).toEqual({ appPolicy: null, projectPolicy: null, preflight });
  });
});
