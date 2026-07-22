import {
  type AgentControlAppPolicy,
  type AgentControlProjectPolicy,
  type AgentControlRole,
  type AgentControlRoleRoute,
  type AgentControlRoleRoutes,
  type ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  type AgentControlConfiguredProviderInstance,
  type AgentControlPolicyResolution,
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

function routes(
  overrides: Partial<Record<AgentControlRole, AgentControlRoleRoute>> = {},
): AgentControlRoleRoutes {
  const base = route([selection(CODEX_INSTANCE, "gpt-5.4")]);
  return {
    orchestrator: overrides.orchestrator ?? base,
    planner: overrides.planner ?? base,
    implementer: overrides.implementer ?? base,
    reviewer: overrides.reviewer ?? base,
    repair: overrides.repair ?? base,
    verifier: overrides.verifier ?? base,
  };
}

function appPolicy(overrides: Partial<AgentControlAppPolicy> = {}): AgentControlAppPolicy {
  return {
    providerAllowlist: [CODEX_INSTANCE, CLAUDE_INSTANCE, DISABLED_INSTANCE],
    roleRoutes: routes(),
    defaultFallbacks: [],
    ...overrides,
  };
}

function resolve(
  input: {
    readonly appPolicy?: AgentControlAppPolicy;
    readonly projectPolicy?: AgentControlProjectPolicy;
  } = {},
): AgentControlPolicyResolution {
  return resolveAgentControlPolicy({
    appPolicy: input.appPolicy ?? appPolicy(),
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
  it("inherits app values and replaces only explicit project values", () => {
    const projectReviewer = route([selection(CLAUDE_INSTANCE, "claude-sonnet-5")], {
      driverKind: CLAUDE_DRIVER,
      strict: false,
    });
    const projectFallback = selection(CLAUDE_INSTANCE, "claude-haiku-4-5");
    const policy = resolvedPolicy(
      resolve({
        projectPolicy: {
          roleRoutes: { reviewer: projectReviewer },
          defaultFallbacks: [projectFallback],
        },
      }),
    );

    expect(policy.providerAllowlist).toEqual([CODEX_INSTANCE, CLAUDE_INSTANCE, DISABLED_INSTANCE]);
    expect(policy.roleRoutes.implementer.candidates).toEqual([
      { source: "role-route", selection: selection(CODEX_INSTANCE, "gpt-5.4") },
    ]);
    expect(policy.roleRoutes.reviewer).toMatchObject({
      driverKind: CLAUDE_DRIVER,
      strict: false,
      candidates: [
        {
          source: "role-route",
          selection: selection(CLAUDE_INSTANCE, "claude-sonnet-5"),
        },
        { source: "default-fallback", selection: projectFallback },
      ],
    });
  });

  it("keeps the provider allowlist separate from project role routing", () => {
    const errors = resolutionErrors(
      resolve({
        projectPolicy: {
          providerAllowlist: [CODEX_INSTANCE],
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

  it("adds global fallbacks only for explicitly non-strict routes", () => {
    const fallback = selection(CLAUDE_INSTANCE, "claude-sonnet-5");
    const policy = resolvedPolicy(
      resolve({
        appPolicy: appPolicy({
          roleRoutes: routes({
            planner: route([selection(CODEX_INSTANCE, "gpt-5.4")], { strict: false }),
          }),
          defaultFallbacks: [fallback],
        }),
      }),
    );

    expect(policy.roleRoutes.orchestrator.candidates).toHaveLength(1);
    expect(policy.roleRoutes.planner.candidates).toEqual([
      { source: "role-route", selection: selection(CODEX_INSTANCE, "gpt-5.4") },
      { source: "default-fallback", selection: fallback },
    ]);
  });

  it("rejects disabled configured provider instances", () => {
    const errors = resolutionErrors(
      resolve({
        appPolicy: appPolicy({
          roleRoutes: routes({
            repair: route([selection(DISABLED_INSTANCE, "gpt-5.4")]),
          }),
        }),
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
        appPolicy: appPolicy({
          roleRoutes: routes({
            reviewer: route([selection(CODEX_INSTANCE, "gpt-5.4")], {
              driverKind: CLAUDE_DRIVER,
            }),
          }),
        }),
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
    const { providerAllowlist: _providerAllowlist, ...policyWithoutAllowlist } = appPolicy({
      roleRoutes: routes({
        implementer: route([
          selection(MISSING_INSTANCE, "gpt-5.4"),
          selection(CODEX_INSTANCE, "gpt-5.3-codex"),
        ]),
      }),
    });
    const errors = resolutionErrors(
      resolve({
        appPolicy: policyWithoutAllowlist,
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
