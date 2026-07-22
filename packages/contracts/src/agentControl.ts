/**
 * Schema-only contracts for Agent Control policy.
 *
 * Provider admission and role/model routing intentionally remain separate:
 * `providerAllowlist` limits concrete configured instances, while role routes
 * contain the ordered model selections that may be considered for a role.
 * Runtime resolution lives outside this package.
 *
 * @module agentControl
 */
import * as Schema from "effect/Schema";

import { ModelSelection } from "./orchestration.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";

export const AGENT_CONTROL_ROLES = [
  "orchestrator",
  "planner",
  "implementer",
  "reviewer",
  "repair",
  "verifier",
] as const;

export const AgentControlRole = Schema.Literals(AGENT_CONTROL_ROLES);
export type AgentControlRole = typeof AgentControlRole.Type;

export const AgentControlAccessMode = Schema.Literals(["restricted", "full-access"]);
export type AgentControlAccessMode = typeof AgentControlAccessMode.Type;

export const AgentControlRoleRoute = Schema.Struct({
  candidates: Schema.Array(ModelSelection).check(
    Schema.isNonEmpty({ message: "An Agent Control role route needs at least one candidate" }),
  ),
  driverKind: Schema.optionalKey(ProviderDriverKind),
  strict: Schema.Boolean,
});
export type AgentControlRoleRoute = typeof AgentControlRoleRoute.Type;

export const AgentControlRoleRoutes = Schema.Struct({
  orchestrator: Schema.optionalKey(AgentControlRoleRoute),
  planner: Schema.optionalKey(AgentControlRoleRoute),
  implementer: Schema.optionalKey(AgentControlRoleRoute),
  reviewer: Schema.optionalKey(AgentControlRoleRoute),
  repair: Schema.optionalKey(AgentControlRoleRoute),
  verifier: Schema.optionalKey(AgentControlRoleRoute),
});
export type AgentControlRoleRoutes = typeof AgentControlRoleRoutes.Type;

export const AgentControlPolicyDefaults = Schema.Struct({
  defaultFallbacks: Schema.Array(ModelSelection).check(
    Schema.isNonEmpty({ message: "Agent Control built-in defaults must not be empty" }),
  ),
});
export type AgentControlPolicyDefaults = typeof AgentControlPolicyDefaults.Type;

/** Optional app-wide overrides layered on top of built-in defaults. */
export const AgentControlAppPolicy = Schema.Struct({
  providerAllowlist: Schema.optionalKey(Schema.Array(ProviderInstanceId)),
  roleRoutes: Schema.optionalKey(AgentControlRoleRoutes),
  defaultFallbacks: Schema.optionalKey(Schema.Array(ModelSelection)),
});
export type AgentControlAppPolicy = typeof AgentControlAppPolicy.Type;

/**
 * Project-local overrides. Missing values inherit from the app policy.
 *
 * Full Access is intentionally one project-level switch rather than a
 * configurable role map. The resolver applies it only to Implementer and
 * Repair; all other roles remain restricted by invariant.
 */
export const AgentControlProjectPolicy = Schema.Struct({
  providerAllowlist: Schema.optionalKey(Schema.Array(ProviderInstanceId)),
  roleRoutes: Schema.optionalKey(AgentControlRoleRoutes),
  defaultFallbacks: Schema.optionalKey(Schema.Array(ModelSelection)),
  fullAccess: Schema.optionalKey(Schema.Boolean),
});
export type AgentControlProjectPolicy = typeof AgentControlProjectPolicy.Type;
