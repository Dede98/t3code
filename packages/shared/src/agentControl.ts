import {
  AGENT_CONTROL_ROLES,
  type AgentControlAccessMode,
  type AgentControlAppPolicy,
  type AgentControlPolicyDefaults,
  type AgentControlProjectPolicy,
  type AgentControlRole,
  type AgentControlRoleRoute,
  type ModelSelection,
  type ProviderDriverKind,
  type ProviderInstanceId,
} from "@t3tools/contracts";

export interface AgentControlConfiguredProviderInstance {
  readonly driverKind: ProviderDriverKind;
  readonly enabled: boolean;
}

export interface AgentControlPolicyResolverInput {
  readonly defaults: AgentControlPolicyDefaults;
  readonly appPolicy?: AgentControlAppPolicy;
  readonly projectPolicy?: AgentControlProjectPolicy;
  /** Presence in this map means the provider instance is configured. */
  readonly providerInstances: ReadonlyMap<
    ProviderInstanceId,
    AgentControlConfiguredProviderInstance
  >;
}

export type AgentControlCandidateSource = "role-route" | "default-fallback";

export interface ResolvedAgentControlCandidate {
  readonly selection: ModelSelection;
  readonly source: AgentControlCandidateSource;
}

export interface ResolvedAgentControlRoleRoute {
  readonly candidates: ReadonlyArray<ResolvedAgentControlCandidate>;
  readonly driverKind?: ProviderDriverKind;
  readonly strict: boolean;
  readonly accessMode: AgentControlAccessMode;
}

export interface ResolvedAgentControlPolicy {
  readonly providerAllowlist?: ReadonlyArray<ProviderInstanceId>;
  readonly defaultFallbacks: ReadonlyArray<ModelSelection>;
  readonly fullAccess: boolean;
  readonly roleRoutes: Readonly<Record<AgentControlRole, ResolvedAgentControlRoleRoute>>;
}

export type AgentControlPolicyResolutionErrorCode =
  | "role-unresolved"
  | "provider-not-allowed"
  | "provider-not-configured"
  | "provider-disabled"
  | "driver-kind-mismatch";

export interface AgentControlPolicyUnresolvedRoleError {
  readonly code: "role-unresolved";
  readonly role: AgentControlRole;
}

export interface AgentControlPolicyCandidateError {
  readonly code: Exclude<AgentControlPolicyResolutionErrorCode, "role-unresolved">;
  readonly role: AgentControlRole;
  readonly source: AgentControlCandidateSource;
  readonly candidateIndex: number;
  readonly instanceId: ProviderInstanceId;
  readonly expectedDriverKind?: ProviderDriverKind;
  readonly actualDriverKind?: ProviderDriverKind;
}

export type AgentControlPolicyResolutionError =
  | AgentControlPolicyUnresolvedRoleError
  | AgentControlPolicyCandidateError;

export type AgentControlPolicyResolution =
  | {
      readonly ok: true;
      readonly policy: ResolvedAgentControlPolicy;
    }
  | {
      readonly ok: false;
      readonly policy: ResolvedAgentControlPolicy;
      readonly errors: ReadonlyArray<AgentControlPolicyResolutionError>;
    };

const FULL_ACCESS_ROLES: ReadonlySet<AgentControlRole> = new Set(["implementer", "repair"]);

function effectiveRoleRoute(
  role: AgentControlRole,
  appPolicy: AgentControlAppPolicy | undefined,
  projectPolicy: AgentControlProjectPolicy | undefined,
): AgentControlRoleRoute | undefined {
  return projectPolicy?.roleRoutes?.[role] ?? appPolicy?.roleRoutes?.[role];
}

function candidateError(input: {
  readonly selection: ModelSelection;
  readonly role: AgentControlRole;
  readonly driverKind: ProviderDriverKind | undefined;
  readonly source: AgentControlCandidateSource;
  readonly candidateIndex: number;
  readonly allowlist: ReadonlySet<ProviderInstanceId> | undefined;
  readonly providerInstances: AgentControlPolicyResolverInput["providerInstances"];
}): AgentControlPolicyResolutionError | undefined {
  const { selection, role, driverKind, source, candidateIndex, allowlist, providerInstances } =
    input;
  const base = {
    role,
    source,
    candidateIndex,
    instanceId: selection.instanceId,
  } as const;

  if (allowlist !== undefined && !allowlist.has(selection.instanceId)) {
    return { code: "provider-not-allowed", ...base };
  }

  const providerInstance = providerInstances.get(selection.instanceId);
  if (providerInstance === undefined) {
    return { code: "provider-not-configured", ...base };
  }
  if (!providerInstance.enabled) {
    return { code: "provider-disabled", ...base };
  }
  if (driverKind !== undefined && providerInstance.driverKind !== driverKind) {
    return {
      code: "driver-kind-mismatch",
      ...base,
      expectedDriverKind: driverKind,
      actualDriverKind: providerInstance.driverKind,
    };
  }

  return undefined;
}

/**
 * Resolves app + project policy without selecting a provider or model.
 *
 * Every explicit candidate is validated and any invalid candidate fails the
 * whole resolution. Candidates are never filtered, substituted, or selected,
 * so later scheduling cannot accidentally turn a policy error into a silent
 * provider/model fallback.
 */
export function resolveAgentControlPolicy(
  input: AgentControlPolicyResolverInput,
): AgentControlPolicyResolution {
  const { appPolicy, projectPolicy, providerInstances } = input;
  const providerAllowlist = projectPolicy?.providerAllowlist ?? appPolicy?.providerAllowlist;
  const defaultFallbacks =
    projectPolicy?.defaultFallbacks ??
    appPolicy?.defaultFallbacks ??
    input.defaults?.defaultFallbacks ??
    [];
  const fullAccess = projectPolicy?.fullAccess === true;
  const allowlist =
    providerAllowlist === undefined ? undefined : new Set<ProviderInstanceId>(providerAllowlist);
  const errors: Array<AgentControlPolicyResolutionError> = [];
  const roleRoutes = {} as Record<AgentControlRole, ResolvedAgentControlRoleRoute>;

  for (const role of AGENT_CONTROL_ROLES) {
    const route = effectiveRoleRoute(role, appPolicy, projectPolicy);
    const routeCandidates: ReadonlyArray<ResolvedAgentControlCandidate> =
      route?.candidates.map((selection) => ({ selection, source: "role-route" })) ?? [];
    const fallbackCandidates: ReadonlyArray<ResolvedAgentControlCandidate> =
      route?.strict === true
        ? []
        : defaultFallbacks.map((selection) => ({ selection, source: "default-fallback" }));
    const candidates = [...routeCandidates, ...fallbackCandidates];

    if (candidates.length === 0) errors.push({ code: "role-unresolved", role });

    for (let candidateIndex = 0; candidateIndex < candidates.length; candidateIndex += 1) {
      const candidate = candidates[candidateIndex];
      if (candidate === undefined) continue;
      const error = candidateError({
        selection: candidate.selection,
        role,
        driverKind: route?.driverKind,
        source: candidate.source,
        candidateIndex,
        allowlist,
        providerInstances,
      });
      if (error !== undefined) errors.push(error);
    }

    roleRoutes[role] = {
      candidates,
      ...(route?.driverKind === undefined ? {} : { driverKind: route.driverKind }),
      strict: route?.strict ?? false,
      accessMode: fullAccess && FULL_ACCESS_ROLES.has(role) ? "full-access" : "restricted",
    };
  }

  const policy: ResolvedAgentControlPolicy = {
    ...(providerAllowlist === undefined ? {} : { providerAllowlist }),
    defaultFallbacks,
    fullAccess,
    roleRoutes,
  };

  return errors.length > 0 ? { ok: false, policy, errors } : { ok: true, policy };
}
