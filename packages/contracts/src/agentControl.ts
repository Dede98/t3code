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

import { IsoDateTime, NonNegativeInt, PositiveInt, ProjectId } from "./baseSchemas.ts";
import { ModelSelection } from "./orchestration.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";

export const AGENT_CONTROL_RPC_METHODS = {
  getPolicy: "agentControl.getPolicy",
  setProjectPolicy: "agentControl.setProjectPolicy",
  clearProjectPolicy: "agentControl.clearProjectPolicy",
  preflightPolicy: "agentControl.preflightPolicy",
  preflightRuntime: "agentControl.preflightRuntime",
} as const;

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

export const AgentControlProjectPolicyState = Schema.Struct({
  projectId: ProjectId,
  policy: AgentControlProjectPolicy,
  revision: PositiveInt,
  updatedAt: IsoDateTime,
});
export type AgentControlProjectPolicyState = typeof AgentControlProjectPolicyState.Type;

export const AgentControlGetPolicyInput = Schema.Struct({
  projectId: ProjectId,
});
export type AgentControlGetPolicyInput = typeof AgentControlGetPolicyInput.Type;

export const AgentControlSetProjectPolicyInput = Schema.Struct({
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
  policy: AgentControlProjectPolicy,
});
export type AgentControlSetProjectPolicyInput = typeof AgentControlSetProjectPolicyInput.Type;

export const AgentControlClearProjectPolicyInput = Schema.Struct({
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
});
export type AgentControlClearProjectPolicyInput = typeof AgentControlClearProjectPolicyInput.Type;

/**
 * Missing draft fields inherit the persisted value, `null` removes that
 * override for this request, and an object is resolved as an ephemeral draft.
 */
export const AgentControlPreflightPolicyInput = Schema.Struct({
  projectId: ProjectId,
  appPolicy: Schema.optionalKey(Schema.NullOr(AgentControlAppPolicy)),
  projectPolicy: Schema.optionalKey(Schema.NullOr(AgentControlProjectPolicy)),
});
export type AgentControlPreflightPolicyInput = typeof AgentControlPreflightPolicyInput.Type;

/** Runtime preflight intentionally has the exact same ephemeral draft semantics. */
export const AgentControlPreflightRuntimeInput = AgentControlPreflightPolicyInput;
export type AgentControlPreflightRuntimeInput = typeof AgentControlPreflightRuntimeInput.Type;

export const AgentControlPreflightCandidateSource = Schema.Literals([
  "role-route",
  "default-fallback",
]);
export type AgentControlPreflightCandidateSource = typeof AgentControlPreflightCandidateSource.Type;

export const AgentControlPreflightCandidate = Schema.Struct({
  selection: ModelSelection,
  source: AgentControlPreflightCandidateSource,
  driverKind: Schema.NullOr(ProviderDriverKind),
});
export type AgentControlPreflightCandidate = typeof AgentControlPreflightCandidate.Type;

export const AgentControlPreflightRole = Schema.Struct({
  role: AgentControlRole,
  accessMode: AgentControlAccessMode,
  strict: Schema.Boolean,
  validCandidates: Schema.Array(AgentControlPreflightCandidate),
});
export type AgentControlPreflightRole = typeof AgentControlPreflightRole.Type;

export const AgentControlPreflightRoleUnresolvedError = Schema.Struct({
  code: Schema.Literal("role-unresolved"),
  role: AgentControlRole,
});
export type AgentControlPreflightRoleUnresolvedError =
  typeof AgentControlPreflightRoleUnresolvedError.Type;

export const AgentControlPreflightCandidateError = Schema.Struct({
  code: Schema.Literals([
    "provider-not-configured",
    "provider-disabled",
    "provider-not-allowed",
    "driver-kind-mismatch",
  ]),
  role: AgentControlRole,
  source: AgentControlPreflightCandidateSource,
  candidateIndex: NonNegativeInt,
  instanceId: ProviderInstanceId,
  expectedDriverKind: Schema.NullOr(ProviderDriverKind),
  actualDriverKind: Schema.NullOr(ProviderDriverKind),
});
export type AgentControlPreflightCandidateError = typeof AgentControlPreflightCandidateError.Type;

export const AgentControlPreflightError = Schema.Union([
  AgentControlPreflightRoleUnresolvedError,
  AgentControlPreflightCandidateError,
]);
export type AgentControlPreflightError = typeof AgentControlPreflightError.Type;

const AgentControlPreflightRoles = Schema.Array(AgentControlPreflightRole);

export const AgentControlPreflightPolicyResult = Schema.Union([
  Schema.Struct({
    ok: Schema.Literal(true),
    roles: AgentControlPreflightRoles,
  }),
  Schema.Struct({
    ok: Schema.Literal(false),
    roles: AgentControlPreflightRoles,
    errors: Schema.Array(AgentControlPreflightError),
  }),
]);
export type AgentControlPreflightPolicyResult = typeof AgentControlPreflightPolicyResult.Type;

export const AgentControlRuntimeErrorCode = Schema.Literals([
  "provider-instance-missing",
  "provider-driver-unavailable",
  "provider-disabled",
  "provider-not-installed",
  "provider-not-ready",
  "provider-unauthenticated",
  "provider-probe-timeout",
  "provider-probe-failed",
  "model-unavailable",
  "driver-kind-mismatch",
  "provider-not-allowed",
  "role-runtime-unresolved",
]);
export type AgentControlRuntimeErrorCode = typeof AgentControlRuntimeErrorCode.Type;

export const AgentControlRuntimeCandidateErrorCode = Schema.Literals([
  "provider-instance-missing",
  "provider-driver-unavailable",
  "provider-disabled",
  "provider-not-installed",
  "provider-not-ready",
  "provider-unauthenticated",
  "provider-probe-timeout",
  "provider-probe-failed",
  "model-unavailable",
  "driver-kind-mismatch",
  "provider-not-allowed",
]);
export type AgentControlRuntimeCandidateErrorCode =
  typeof AgentControlRuntimeCandidateErrorCode.Type;

export const AgentControlRuntimeProviderStatus = Schema.Literals([
  "ready",
  "warning",
  "error",
  "disabled",
]);
export type AgentControlRuntimeProviderStatus = typeof AgentControlRuntimeProviderStatus.Type;

export const AgentControlRuntimeAuthStatus = Schema.Literals([
  "unknown",
  "authenticated",
  "unauthenticated",
]);
export type AgentControlRuntimeAuthStatus = typeof AgentControlRuntimeAuthStatus.Type;

export const AgentControlPreflightRuntimeCandidate = Schema.Struct({
  candidateIndex: NonNegativeInt,
  source: AgentControlPreflightCandidateSource,
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
  driverKind: Schema.NullOr(ProviderDriverKind),
  providerStatus: Schema.NullOr(AgentControlRuntimeProviderStatus),
  authStatus: Schema.NullOr(AgentControlRuntimeAuthStatus),
  checkedAt: Schema.NullOr(IsoDateTime),
  runtimeReady: Schema.Boolean,
  errorCode: Schema.NullOr(AgentControlRuntimeCandidateErrorCode),
});
export type AgentControlPreflightRuntimeCandidate =
  typeof AgentControlPreflightRuntimeCandidate.Type;

export const AgentControlPreflightRuntimeRole = Schema.Struct({
  role: AgentControlRole,
  accessMode: AgentControlAccessMode,
  strict: Schema.Boolean,
  candidates: Schema.Array(AgentControlPreflightRuntimeCandidate),
  selectedCandidateIndex: Schema.NullOr(NonNegativeInt),
  errorCode: Schema.NullOr(Schema.Literal("role-runtime-unresolved")),
});
export type AgentControlPreflightRuntimeRole = typeof AgentControlPreflightRuntimeRole.Type;

export const AgentControlPreflightRuntimeResult = Schema.Struct({
  ok: Schema.Boolean,
  staticPreflight: AgentControlPreflightPolicyResult,
  roles: Schema.Array(AgentControlPreflightRuntimeRole),
});
export type AgentControlPreflightRuntimeResult = typeof AgentControlPreflightRuntimeResult.Type;

export const AgentControlPolicyStateResult = Schema.Struct({
  appPolicy: Schema.NullOr(AgentControlAppPolicy),
  projectPolicy: Schema.NullOr(AgentControlProjectPolicyState),
  preflight: AgentControlPreflightPolicyResult,
});
export type AgentControlPolicyStateResult = typeof AgentControlPolicyStateResult.Type;

export class AgentControlPolicyValidationError extends Schema.TaggedErrorClass<AgentControlPolicyValidationError>()(
  "AgentControlPolicyValidationError",
  {
    code: Schema.Literal("validation"),
    operation: Schema.Literals([
      "get-policy",
      "set-project-policy",
      "clear-project-policy",
      "preflight-policy",
      "preflight-runtime",
    ]),
  },
) {}

export class AgentControlPolicyRevisionConflictError extends Schema.TaggedErrorClass<AgentControlPolicyRevisionConflictError>()(
  "AgentControlPolicyRevisionConflictError",
  {
    code: Schema.Literal("revision-conflict"),
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: Schema.NullOr(PositiveInt),
  },
) {}

export class AgentControlPolicyProjectMissingError extends Schema.TaggedErrorClass<AgentControlPolicyProjectMissingError>()(
  "AgentControlPolicyProjectMissingError",
  {
    code: Schema.Literal("project-missing"),
    projectId: ProjectId,
  },
) {}

export class AgentControlPolicyProjectDeletedError extends Schema.TaggedErrorClass<AgentControlPolicyProjectDeletedError>()(
  "AgentControlPolicyProjectDeletedError",
  {
    code: Schema.Literal("project-deleted"),
    projectId: ProjectId,
  },
) {}

export class AgentControlPolicyCorruptError extends Schema.TaggedErrorClass<AgentControlPolicyCorruptError>()(
  "AgentControlPolicyCorruptError",
  {
    code: Schema.Literal("policy-corrupt"),
    projectId: ProjectId,
  },
) {}

export class AgentControlPolicyPersistenceError extends Schema.TaggedErrorClass<AgentControlPolicyPersistenceError>()(
  "AgentControlPolicyPersistenceError",
  {
    code: Schema.Literal("internal-persistence-error"),
    operation: Schema.Literals([
      "get-policy",
      "set-project-policy",
      "clear-project-policy",
      "preflight-policy",
      "preflight-runtime",
    ]),
  },
) {}

export const AgentControlPolicyRpcError = Schema.Union([
  AgentControlPolicyValidationError,
  AgentControlPolicyRevisionConflictError,
  AgentControlPolicyProjectMissingError,
  AgentControlPolicyProjectDeletedError,
  AgentControlPolicyCorruptError,
  AgentControlPolicyPersistenceError,
]);
export type AgentControlPolicyRpcError = typeof AgentControlPolicyRpcError.Type;
