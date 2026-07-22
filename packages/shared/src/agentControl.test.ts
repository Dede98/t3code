import {
  AGENT_CONTROL_ROLES,
  type AgentControlAppPolicy,
  type AgentControlPolicyDefaults,
  type AgentControlProjectPolicy,
  type AgentControlRoleRoute,
  type ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  type AgentControlConfiguredProviderInstance,
  type AgentControlPolicyResolution,
  type AgentControlPolicyResolverInput,
  resolveAgentControlPolicy,
} from "./agentControl.ts";

const CODEX_INSTANCE = ProviderInstanceId.make("codex-work");
const CLAUDE_INSTANCE = ProviderInstanceId.make("claude-work");
const DISABLED_INSTANCE = ProviderInstanceId.make("codex-disabled");
const MISSING_INSTANCE = ProviderInstanceId.make("codex-missing");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");

const providerInstances = new Map<ProviderInstanceId, AgentControlConfiguredProviderInstance>([
  [CODEX_INSTANCE, { driverKind: CODEX_DRIVER, enabled: true }],
  [CLAUDE_INSTANCE, { driverKind: CLAUDE_DRIVER, enabled: true }],
  [DISABLED_INSTANCE, { driverKind: CODEX_DRIVER, enabled: false }],
]);

function selection(instanceId: ProviderInstanceId, model: string): ModelSelection {
  return { instanceId, model };
}

function route(
  candidates: ReadonlyArray<ModelSelection>,
  overrides: Partial<Pick<AgentControlRoleRoute, "driverKind" | "strict">> = {},
): AgentControlRoleRoute {
  return {
    candidates,
    strict: overrides.strict ?? true,
    ...(overrides.driverKind === undefined ? {} : { driverKind: overrides.driverKind }),
  };
}

function builtInDefaults(
  defaultFallbacks: ReadonlyArray<ModelSelection> = [selection(CODEX_INSTANCE, "gpt-5.4")],
): AgentControlPolicyDefaults {
  return { defaultFallbacks };
}

function resolve(
  input: {
    readonly defaults?: AgentControlPolicyDefaults;
    readonly appPolicy?: AgentControlAppPolicy;
    readonly projectPolicy?: AgentControlProjectPolicy;
  } = {},
): AgentControlPolicyResolution {
  return resolveAgentControlPolicy({
    defaults: input.defaults ?? builtInDefaults(),
    ...(input.appPolicy === undefined ? {} : { appPolicy: input.appPolicy }),
    ...(input.projectPolicy === undefined ? {} : { projectPolicy: input.projectPolicy }),
    providerInstances,
  });
}

function resolvedPolicy(result: AgentControlPolicyResolution) {
  expect(result.ok).toBe(true);
  if (!result.ok)
    throw new Error(`Expected resolution success, got ${result.errors.length} errors`);
  return result.policy;
}

function resolutionErrors(result: AgentControlPolicyResolution) {
  expect(result.ok).toBe(false);
  if (result.ok) throw new Error("Expected resolution failure");
  return result.errors;
}

describe("resolveAgentControlPolicy", () => {
  it("uses built-in fallbacks for every role with an empty app policy", () => {
    const fallback = selection(CODEX_INSTANCE, "gpt-5.4");
    const policy = resolvedPolicy(
      resolve({
        defaults: builtInDefaults([fallback]),
        appPolicy: {},
      }),
    );

    for (const role of AGENT_CONTROL_ROLES) {
      expect(policy.roleRoutes[role]).toMatchObject({
        strict: false,
        candidates: [{ source: "default-fallback", selection: fallback }],
      });
    }
  });

  it("lets the app override only Reviewer", () => {
    const reviewerSelection = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const policy = resolvedPolicy(
      resolve({
        appPolicy: {
          roleRoutes: {
            reviewer: route([reviewerSelection], {
              driverKind: CLAUDE_DRIVER,
            }),
          },
        },
      }),
    );

    expect(policy.roleRoutes.reviewer).toMatchObject({
      driverKind: CLAUDE_DRIVER,
      strict: true,
      candidates: [{ source: "role-route", selection: reviewerSelection }],
    });
    expect(policy.roleRoutes.planner.candidates).toEqual([
      {
        source: "default-fallback",
        selection: selection(CODEX_INSTANCE, "gpt-5.4"),
      },
    ]);
  });

  it("lets the project override only Implementer while preserving app routes", () => {
    const appImplementer = selection(CODEX_INSTANCE, "gpt-5.3-codex");
    const appReviewer = selection(CODEX_INSTANCE, "gpt-5.4");
    const projectImplementer = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const policy = resolvedPolicy(
      resolve({
        appPolicy: {
          roleRoutes: {
            implementer: route([appImplementer]),
            reviewer: route([appReviewer]),
          },
        },
        projectPolicy: {
          roleRoutes: {
            implementer: route([projectImplementer]),
          },
        },
      }),
    );

    expect(policy.roleRoutes.implementer.candidates).toEqual([
      { source: "role-route", selection: projectImplementer },
    ]);
    expect(policy.roleRoutes.reviewer.candidates).toEqual([
      { source: "role-route", selection: appReviewer },
    ]);
  });

  it("prefers project fallbacks over app and built-in fallbacks", () => {
    const builtInFallback = selection(CODEX_INSTANCE, "gpt-5.4");
    const appFallback = selection(CODEX_INSTANCE, "gpt-5.3-codex");
    const projectFallback = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const policy = resolvedPolicy(
      resolve({
        defaults: builtInDefaults([builtInFallback]),
        appPolicy: { defaultFallbacks: [appFallback] },
        projectPolicy: { defaultFallbacks: [projectFallback] },
      }),
    );

    expect(policy.defaultFallbacks).toEqual([projectFallback]);
    for (const role of AGENT_CONTROL_ROLES) {
      expect(policy.roleRoutes[role].candidates).toEqual([
        { source: "default-fallback", selection: projectFallback },
      ]);
    }
  });

  it("does not add defaults to a strict route", () => {
    const strictSelection = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const policy = resolvedPolicy(
      resolve({
        appPolicy: {
          roleRoutes: {
            reviewer: route([strictSelection], { strict: true }),
          },
        },
      }),
    );

    expect(policy.roleRoutes.reviewer.candidates).toEqual([
      { source: "role-route", selection: strictSelection },
    ]);
  });

  it("appends effective defaults to a non-strict route", () => {
    const routeSelection = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const fallback = selection(CODEX_INSTANCE, "gpt-5.4");
    const policy = resolvedPolicy(
      resolve({
        defaults: builtInDefaults([fallback]),
        appPolicy: {
          roleRoutes: {
            planner: route([routeSelection], { strict: false }),
          },
        },
      }),
    );

    expect(policy.roleRoutes.planner.candidates).toEqual([
      { source: "role-route", selection: routeSelection },
      { source: "default-fallback", selection: fallback },
    ]);
  });

  it("fails closed when effective defaults are empty", () => {
    const errors = resolutionErrors(resolve({ projectPolicy: { defaultFallbacks: [] } }));

    expect(errors).toEqual(AGENT_CONTROL_ROLES.map((role) => ({ code: "role-unresolved", role })));
  });

  it("fails closed when the required defaults input is missing at runtime", () => {
    const errors = resolutionErrors(
      resolveAgentControlPolicy({
        appPolicy: {},
        providerInstances,
      } as unknown as AgentControlPolicyResolverInput),
    );

    expect(errors).toEqual(AGENT_CONTROL_ROLES.map((role) => ({ code: "role-unresolved", role })));
  });

  it("keeps the provider allowlist separate from role routing", () => {
    const errors = resolutionErrors(
      resolve({
        appPolicy: { providerAllowlist: [CODEX_INSTANCE] },
        projectPolicy: {
          roleRoutes: {
            implementer: route([selection(CLAUDE_INSTANCE, "claude-sonnet-5")]),
          },
        },
      }),
    );

    expect(errors).toContainEqual({
      code: "provider-not-allowed",
      role: "implementer",
      source: "role-route",
      candidateIndex: 0,
      instanceId: CLAUDE_INSTANCE,
    });
  });

  it("rejects disabled configured provider instances", () => {
    const errors = resolutionErrors(
      resolve({
        projectPolicy: {
          roleRoutes: {
            repair: route([selection(DISABLED_INSTANCE, "gpt-5.4")]),
          },
        },
      }),
    );

    expect(errors).toContainEqual({
      code: "provider-disabled",
      role: "repair",
      source: "role-route",
      candidateIndex: 0,
      instanceId: DISABLED_INSTANCE,
    });
  });

  it("rejects candidates that violate a route driver constraint", () => {
    const errors = resolutionErrors(
      resolve({
        appPolicy: {
          roleRoutes: {
            reviewer: route([selection(CODEX_INSTANCE, "gpt-5.4")], {
              driverKind: CLAUDE_DRIVER,
            }),
          },
        },
      }),
    );

    expect(errors).toContainEqual({
      code: "driver-constraint-mismatch",
      role: "reviewer",
      source: "role-route",
      candidateIndex: 0,
      instanceId: CODEX_INSTANCE,
      expectedDriverKind: CLAUDE_DRIVER,
      actualDriverKind: CODEX_DRIVER,
    });
  });

  it("fails closed instead of dropping an invalid candidate for a later one", () => {
    const errors = resolutionErrors(
      resolve({
        projectPolicy: {
          roleRoutes: {
            implementer: route([
              selection(MISSING_INSTANCE, "gpt-5.4"),
              selection(CODEX_INSTANCE, "gpt-5.3-codex"),
            ]),
          },
        },
      }),
    );

    expect(errors).toContainEqual({
      code: "provider-not-configured",
      role: "implementer",
      source: "role-route",
      candidateIndex: 0,
      instanceId: MISSING_INSTANCE,
    });
  });

  it("grants project Full Access only to Implementer and Repair", () => {
    const policy = resolvedPolicy(resolve({ projectPolicy: { fullAccess: true } }));

    expect(
      Object.fromEntries(
        Object.entries(policy.roleRoutes).map(([role, resolvedRoute]) => [
          role,
          resolvedRoute.accessMode,
        ]),
      ),
    ).toEqual({
      orchestrator: "restricted",
      planner: "restricted",
      implementer: "full-access",
      reviewer: "restricted",
      repair: "full-access",
      verifier: "restricted",
    });
  });
});
