import {
  AGENT_CONTROL_ROLES,
  AgentControlClearProjectPolicyInput,
  AgentControlGetPolicyInput,
  AgentControlPolicyCorruptError,
  AgentControlPolicyPersistenceError,
  AgentControlPolicyProjectDeletedError,
  AgentControlPolicyProjectMissingError,
  type AgentControlPolicyRpcError,
  type AgentControlPolicyStateResult,
  AgentControlPolicyRevisionConflictError,
  AgentControlPolicyValidationError,
  AgentControlPreflightPolicyInput,
  type AgentControlPreflightPolicyResult,
  AgentControlPreflightRuntimeInput,
  type AgentControlPreflightRuntimeCandidate,
  type AgentControlPreflightRuntimeResult,
  type AgentControlRuntimeCandidateErrorCode,
  AgentControlSetProjectPolicyInput,
  type AgentControlAppPolicy,
  type AgentControlPreflightError,
  type AgentControlProjectPolicy,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import {
  resolveAgentControlPolicy,
  type AgentControlConfiguredProviderInstance,
  type AgentControlPolicyResolution,
  type ResolvedAgentControlCandidate,
} from "@t3tools/shared/agentControl";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  AgentControlProjectPolicyRepository,
  type AgentControlProjectPolicyRecord,
  type ClearAgentControlProjectPolicyError,
  type EnsureAgentControlProjectAvailableError,
  type GetAgentControlProjectPolicyError,
  type SetAgentControlProjectPolicyError,
} from "../persistence/Services/AgentControlProjectPolicies.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import { ServerSettingsService } from "../serverSettings.ts";

type PolicyOperation =
  | "get-policy"
  | "set-project-policy"
  | "clear-project-policy"
  | "preflight-policy"
  | "preflight-runtime";

type RepositoryError =
  | ClearAgentControlProjectPolicyError
  | EnsureAgentControlProjectAvailableError
  | GetAgentControlProjectPolicyError
  | SetAgentControlProjectPolicyError;

type ResolutionContext = {
  readonly settings: ServerSettings;
  readonly providerInstances: ReadonlyMap<
    ProviderInstanceId,
    AgentControlConfiguredProviderInstance
  >;
  readonly unavailableInstances: ReadonlyMap<ProviderInstanceId, ServerProvider>;
};

type RuntimeProbeObservation = {
  readonly driverKind: ProviderDriverKind | null;
  readonly snapshot: ServerProvider | null;
  readonly errorCode: AgentControlRuntimeCandidateErrorCode | null;
};

export const AGENT_CONTROL_RUNTIME_PROBE_TIMEOUT_MS = 45_000;
const AGENT_CONTROL_RUNTIME_PROBE_CONCURRENCY = 2;

const decodeGetPolicyInput = Schema.decodeUnknownEffect(AgentControlGetPolicyInput);
const decodeSetProjectPolicyInput = Schema.decodeUnknownEffect(AgentControlSetProjectPolicyInput);
const decodeClearProjectPolicyInput = Schema.decodeUnknownEffect(
  AgentControlClearProjectPolicyInput,
);
const decodePreflightPolicyInput = Schema.decodeUnknownEffect(AgentControlPreflightPolicyInput);
const decodePreflightRuntimeInput = Schema.decodeUnknownEffect(AgentControlPreflightRuntimeInput);

function validationError(operation: PolicyOperation): AgentControlPolicyValidationError {
  return new AgentControlPolicyValidationError({ code: "validation", operation });
}

function persistenceError(operation: PolicyOperation): AgentControlPolicyPersistenceError {
  return new AgentControlPolicyPersistenceError({
    code: "internal-persistence-error",
    operation,
  });
}

const catchPolicyDefect = (operation: PolicyOperation) =>
  Effect.catchDefect((defect) =>
    Effect.logError("Agent Control policy operation defected", {
      operation,
      defect,
    }).pipe(Effect.andThen(Effect.fail(persistenceError(operation)))),
  );

function mapRepositoryError(
  operation: PolicyOperation,
  error: RepositoryError,
): AgentControlPolicyRpcError {
  switch (error._tag) {
    case "AgentControlProjectPolicyValidationError":
      return validationError(operation);
    case "AgentControlProjectPolicyConflictError":
      return new AgentControlPolicyRevisionConflictError({
        code: "revision-conflict",
        projectId: error.projectId,
        expectedRevision: error.expectedRevision,
        actualRevision: error.actualRevision,
      });
    case "AgentControlProjectPolicyProjectUnavailableError":
      return error.reason === "missing"
        ? new AgentControlPolicyProjectMissingError({
            code: "project-missing",
            projectId: error.projectId,
          })
        : new AgentControlPolicyProjectDeletedError({
            code: "project-deleted",
            projectId: error.projectId,
          });
    case "AgentControlProjectPolicyCorruptError":
      return new AgentControlPolicyCorruptError({
        code: "policy-corrupt",
        projectId: error.projectId,
      });
    case "PersistenceSqlError":
      return persistenceError(operation);
  }
}

function projectPolicyState(
  record: AgentControlProjectPolicyRecord | undefined,
): AgentControlPolicyStateResult["projectPolicy"] {
  return record === undefined
    ? null
    : {
        projectId: record.projectId,
        policy: record.policy,
        revision: record.revision,
        updatedAt: record.updatedAt,
      };
}

function configuredUnavailableInstanceEnabled(
  settings: ServerSettings,
  instanceId: ProviderInstanceId,
): boolean {
  const entry = settings.providerInstances[instanceId];
  if (entry?.enabled !== undefined) return entry.enabled;
  if (entry?.config && typeof entry.config === "object" && !Array.isArray(entry.config)) {
    const enabled = (entry.config as { readonly enabled?: unknown }).enabled;
    if (typeof enabled === "boolean") return enabled;
  }
  return true;
}

function preflightResult(
  resolution: AgentControlPolicyResolution,
  providerInstances: ResolutionContext["providerInstances"],
): AgentControlPreflightPolicyResult {
  const errors: ReadonlyArray<AgentControlPreflightError> = resolution.ok
    ? []
    : resolution.errors.map((error) =>
        error.code === "role-unresolved"
          ? { code: error.code, role: error.role }
          : {
              code: error.code,
              role: error.role,
              source: error.source,
              candidateIndex: error.candidateIndex,
              instanceId: error.instanceId,
              expectedDriverKind: error.expectedDriverKind ?? null,
              actualDriverKind: error.actualDriverKind ?? null,
            },
      );
  const invalidCandidateIndexes = new Map<string, Set<number>>();
  if (!resolution.ok) {
    for (const error of resolution.errors) {
      if (error.code === "role-unresolved") continue;
      const indexes = invalidCandidateIndexes.get(error.role) ?? new Set<number>();
      indexes.add(error.candidateIndex);
      invalidCandidateIndexes.set(error.role, indexes);
    }
  }

  const roles = AGENT_CONTROL_ROLES.map((role) => {
    const route = resolution.policy.roleRoutes[role];
    const invalidIndexes = invalidCandidateIndexes.get(role);
    return {
      role,
      accessMode: route.accessMode,
      strict: route.strict,
      validCandidates: route.candidates.flatMap((candidate, candidateIndex) => {
        if (invalidIndexes?.has(candidateIndex) === true) return [];
        return [
          {
            selection: candidate.selection,
            source: candidate.source,
            driverKind: providerInstances.get(candidate.selection.instanceId)?.driverKind ?? null,
          },
        ];
      }),
    };
  });

  return resolution.ok ? { ok: true, roles } : { ok: false, roles, errors };
}

function resolvePolicyResolution(input: {
  readonly context: ResolutionContext;
  readonly appPolicy: AgentControlAppPolicy | undefined;
  readonly projectPolicy: AgentControlProjectPolicy | undefined;
}): AgentControlPolicyResolution {
  return resolveAgentControlPolicy({
    defaults: {
      defaultFallbacks: [input.context.settings.textGenerationModelSelection],
    },
    ...(input.appPolicy === undefined ? {} : { appPolicy: input.appPolicy }),
    ...(input.projectPolicy === undefined ? {} : { projectPolicy: input.projectPolicy }),
    providerInstances: input.context.providerInstances,
  });
}

function resolvePolicy(input: {
  readonly context: ResolutionContext;
  readonly appPolicy: AgentControlAppPolicy | undefined;
  readonly projectPolicy: AgentControlProjectPolicy | undefined;
}): AgentControlPreflightPolicyResult {
  return preflightResult(resolvePolicyResolution(input), input.context.providerInstances);
}

function runtimeCandidateFromStaticCandidate(input: {
  readonly candidateIndex: number;
  readonly candidate: AgentControlPreflightPolicyResult["roles"][number]["validCandidates"][number];
}): AgentControlPreflightRuntimeCandidate {
  return {
    candidateIndex: input.candidateIndex,
    source: input.candidate.source,
    providerInstanceId: input.candidate.selection.instanceId,
    model: input.candidate.selection.model,
    driverKind: input.candidate.driverKind,
    providerStatus: null,
    authStatus: null,
    checkedAt: null,
    runtimeReady: false,
    errorCode: null,
  };
}

function staticFailureRuntimeResult(
  staticPreflight: AgentControlPreflightPolicyResult,
): AgentControlPreflightRuntimeResult {
  return {
    ok: false,
    staticPreflight,
    roles: staticPreflight.roles.map((role) => ({
      role: role.role,
      accessMode: role.accessMode,
      strict: role.strict,
      candidates: role.validCandidates.map((candidate, candidateIndex) =>
        runtimeCandidateFromStaticCandidate({ candidateIndex, candidate }),
      ),
      selectedCandidateIndex: null,
      errorCode: null,
    })),
  };
}

interface RuntimeEligibleCandidate {
  readonly candidate: ResolvedAgentControlCandidate;
  readonly originalCandidateIndex: number;
}

function runtimeEligibleCandidates(
  resolution: AgentControlPolicyResolution,
): ReadonlyMap<string, ReadonlyArray<RuntimeEligibleCandidate>> {
  const invalidCandidateIndexes = new Map<string, Set<number>>();
  if (!resolution.ok) {
    for (const error of resolution.errors) {
      if (error.code === "role-unresolved") continue;
      const indexes = invalidCandidateIndexes.get(error.role) ?? new Set<number>();
      indexes.add(error.candidateIndex);
      invalidCandidateIndexes.set(error.role, indexes);
    }
  }

  return new Map(
    AGENT_CONTROL_ROLES.map((role) => {
      const invalidIndexes = invalidCandidateIndexes.get(role);
      const candidates = resolution.policy.roleRoutes[role].candidates.flatMap(
        (candidate, originalCandidateIndex) =>
          invalidIndexes?.has(originalCandidateIndex) === true
            ? []
            : [{ candidate, originalCandidateIndex } satisfies RuntimeEligibleCandidate],
      );
      return [role, candidates] as const;
    }),
  );
}

function canProbeRuntimeCandidates(
  resolution: AgentControlPolicyResolution,
  candidatesByRole: ReadonlyMap<string, ReadonlyArray<RuntimeEligibleCandidate>>,
): boolean {
  if (resolution.ok) return true;
  if (resolution.errors.some((error) => error.code === "role-unresolved")) return false;

  for (const role of AGENT_CONTROL_ROLES) {
    const route = resolution.policy.roleRoutes[role];
    const roleHasStaticError = resolution.errors.some(
      (error) => error.code !== "role-unresolved" && error.role === role,
    );
    if ((route.strict && roleHasStaticError) || (candidatesByRole.get(role)?.length ?? 0) === 0) {
      return false;
    }
  }
  return true;
}

export interface AgentControlPolicyServiceShape {
  readonly getPolicy: (
    input: AgentControlGetPolicyInput,
  ) => Effect.Effect<AgentControlPolicyStateResult, AgentControlPolicyRpcError>;
  readonly setProjectPolicy: (
    input: AgentControlSetProjectPolicyInput,
  ) => Effect.Effect<AgentControlPolicyStateResult, AgentControlPolicyRpcError>;
  readonly clearProjectPolicy: (
    input: AgentControlClearProjectPolicyInput,
  ) => Effect.Effect<AgentControlPolicyStateResult, AgentControlPolicyRpcError>;
  readonly preflightPolicy: (
    input: AgentControlPreflightPolicyInput,
  ) => Effect.Effect<AgentControlPreflightPolicyResult, AgentControlPolicyRpcError>;
  readonly preflightRuntime: (
    input: AgentControlPreflightRuntimeInput,
  ) => Effect.Effect<AgentControlPreflightRuntimeResult, AgentControlPolicyRpcError>;
}

export class AgentControlPolicyService extends Context.Service<
  AgentControlPolicyService,
  AgentControlPolicyServiceShape
>()("t3/agentControl/AgentControlPolicyService") {}

const makeAgentControlPolicyService = Effect.gen(function* () {
  const repository = yield* AgentControlProjectPolicyRepository;
  const serverSettings = yield* ServerSettingsService;
  const providerRegistry = yield* ProviderInstanceRegistry;

  const loadResolutionContext = Effect.fn("AgentControlPolicyService.loadResolutionContext")(
    function* (operation: PolicyOperation) {
      const settings = yield* serverSettings.getSettings.pipe(
        Effect.tapError((error) =>
          Effect.logError("Failed to load Agent Control policy settings", {
            operation,
            error,
          }),
        ),
        Effect.mapError(() => persistenceError(operation)),
      );
      const [liveInstances, unavailableSnapshots] = yield* Effect.all([
        providerRegistry.listInstances,
        providerRegistry.listUnavailable,
      ]);
      const providerInstances = new Map<
        ProviderInstanceId,
        AgentControlConfiguredProviderInstance
      >();
      const unavailableInstances = new Map<ProviderInstanceId, ServerProvider>();
      for (const instance of liveInstances) {
        providerInstances.set(instance.instanceId, {
          driverKind: instance.driverKind,
          enabled: instance.enabled,
        });
      }
      for (const instance of unavailableSnapshots) {
        if (!providerInstances.has(instance.instanceId)) {
          providerInstances.set(instance.instanceId, {
            driverKind: instance.driver,
            enabled: configuredUnavailableInstanceEnabled(settings, instance.instanceId),
          });
        }
        unavailableInstances.set(instance.instanceId, instance);
      }
      return { settings, providerInstances, unavailableInstances } satisfies ResolutionContext;
    },
  );

  const mapRepositoryFailure =
    (operation: PolicyOperation) =>
    <A, E extends RepositoryError, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, AgentControlPolicyRpcError, R> =>
      effect.pipe(
        Effect.tapError((error) =>
          Effect.logError("Agent Control policy persistence operation failed", {
            operation,
            error,
          }),
        ),
        Effect.mapError((error) => mapRepositoryError(operation, error)),
      );

  const probeRuntimeInstance = Effect.fn("AgentControlPolicyService.probeRuntimeInstance")(
    function* (
      instanceId: ProviderInstanceId,
      context: ResolutionContext,
    ): Effect.fn.Return<RuntimeProbeObservation> {
      const unavailableSnapshot = context.unavailableInstances.get(instanceId);
      const instance = yield* providerRegistry.getInstance(instanceId);
      if (instance === undefined) {
        return unavailableSnapshot === undefined
          ? {
              driverKind: context.providerInstances.get(instanceId)?.driverKind ?? null,
              snapshot: null,
              errorCode: "provider-instance-missing",
            }
          : {
              driverKind: unavailableSnapshot.driver,
              snapshot: unavailableSnapshot,
              errorCode: "provider-driver-unavailable",
            };
      }

      const staticallyResolvedDriver = context.providerInstances.get(instanceId)?.driverKind;
      if (
        staticallyResolvedDriver !== undefined &&
        instance.driverKind !== staticallyResolvedDriver
      ) {
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "driver-kind-mismatch",
        };
      }
      if (!instance.enabled) {
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "provider-disabled",
        };
      }

      const probeExit = yield* instance.snapshot.refresh.pipe(
        Effect.timeoutOption(Duration.millis(AGENT_CONTROL_RUNTIME_PROBE_TIMEOUT_MS)),
        Effect.exit,
      );
      if (!Exit.isSuccess(probeExit)) {
        yield* Effect.logWarning("Agent Control provider runtime probe failed", {
          instanceId,
          driverKind: instance.driverKind,
        });
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "provider-probe-failed",
        };
      }
      if (Option.isNone(probeExit.value)) {
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "provider-probe-timeout",
        };
      }

      const snapshot = probeExit.value.value;
      const currentInstance = yield* providerRegistry.getInstance(instanceId);
      if (currentInstance === undefined) {
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "provider-instance-missing",
        };
      }
      if (currentInstance !== instance) {
        return {
          driverKind: currentInstance.driverKind,
          snapshot: null,
          errorCode: "provider-probe-failed",
        };
      }
      if (snapshot.instanceId !== instanceId || snapshot.driver !== instance.driverKind) {
        return {
          driverKind: instance.driverKind,
          snapshot: null,
          errorCode: "provider-probe-failed",
        };
      }
      if (snapshot.availability === "unavailable") {
        return {
          driverKind: instance.driverKind,
          snapshot,
          errorCode: "provider-driver-unavailable",
        };
      }
      if (!currentInstance.enabled || !snapshot.enabled || snapshot.status === "disabled") {
        return {
          driverKind: instance.driverKind,
          snapshot,
          errorCode: "provider-disabled",
        };
      }

      return {
        driverKind: instance.driverKind,
        snapshot,
        errorCode: null,
      };
    },
  );

  const projectRuntimeCandidate = (input: {
    readonly candidateIndex: number;
    readonly candidate: ResolvedAgentControlCandidate;
    readonly expectedDriverKind: ProviderDriverKind | undefined;
    readonly allowlist: ReadonlySet<ProviderInstanceId> | undefined;
    readonly observation: RuntimeProbeObservation;
  }): AgentControlPreflightRuntimeCandidate => {
    const { candidate, candidateIndex, expectedDriverKind, allowlist, observation } = input;
    const snapshot = observation.snapshot;
    let errorCode = observation.errorCode;

    if (errorCode === null && snapshot === null) {
      errorCode = "provider-probe-failed";
    }
    if (errorCode === null && snapshot !== null && !snapshot.installed) {
      errorCode = "provider-not-installed";
    }
    if (errorCode === null && snapshot?.auth.status === "unauthenticated") {
      errorCode = "provider-unauthenticated";
    }
    if (errorCode === null && snapshot?.status !== "ready") {
      errorCode = "provider-not-ready";
    }
    if (
      errorCode === null &&
      snapshot !== null &&
      !snapshot.models.some((model) => model.slug === candidate.selection.model)
    ) {
      errorCode = "model-unavailable";
    }
    if (
      errorCode === null &&
      expectedDriverKind !== undefined &&
      observation.driverKind !== expectedDriverKind
    ) {
      errorCode = "driver-kind-mismatch";
    }
    if (
      errorCode === null &&
      allowlist !== undefined &&
      !allowlist.has(candidate.selection.instanceId)
    ) {
      errorCode = "provider-not-allowed";
    }

    return {
      candidateIndex,
      source: candidate.source,
      providerInstanceId: candidate.selection.instanceId,
      model: candidate.selection.model,
      driverKind: observation.driverKind,
      providerStatus: snapshot?.status ?? null,
      authStatus: snapshot?.auth.status ?? null,
      checkedAt: snapshot?.checkedAt ?? null,
      runtimeReady: errorCode === null,
      errorCode,
    };
  };

  const getPolicy = Effect.fn("AgentControlPolicyService.getPolicy")(function* (
    rawInput: AgentControlGetPolicyInput,
  ) {
    const operation = "get-policy" as const;
    const input = yield* decodeGetPolicyInput(rawInput).pipe(
      Effect.mapError(() => validationError(operation)),
    );
    yield* repository.ensureProjectAvailable(input.projectId).pipe(mapRepositoryFailure(operation));
    const recordOption = yield* repository
      .getProjectPolicy(input.projectId)
      .pipe(mapRepositoryFailure(operation));
    const record = Option.getOrUndefined(recordOption);
    const context = yield* loadResolutionContext(operation);
    return {
      appPolicy: context.settings.agentControlPolicy ?? null,
      projectPolicy: projectPolicyState(record),
      preflight: resolvePolicy({
        context,
        appPolicy: context.settings.agentControlPolicy,
        projectPolicy: record?.policy,
      }),
    } satisfies AgentControlPolicyStateResult;
  }, catchPolicyDefect("get-policy"));

  const setProjectPolicy = Effect.fn("AgentControlPolicyService.setProjectPolicy")(function* (
    rawInput: AgentControlSetProjectPolicyInput,
  ) {
    const operation = "set-project-policy" as const;
    const input = yield* decodeSetProjectPolicyInput(rawInput).pipe(
      Effect.mapError(() => validationError(operation)),
    );
    const context = yield* loadResolutionContext(operation);
    const record = yield* repository.setProjectPolicy(input).pipe(mapRepositoryFailure(operation));
    return {
      appPolicy: context.settings.agentControlPolicy ?? null,
      projectPolicy: projectPolicyState(record),
      preflight: resolvePolicy({
        context,
        appPolicy: context.settings.agentControlPolicy,
        projectPolicy: record.policy,
      }),
    } satisfies AgentControlPolicyStateResult;
  }, catchPolicyDefect("set-project-policy"));

  const clearProjectPolicy = Effect.fn("AgentControlPolicyService.clearProjectPolicy")(function* (
    rawInput: AgentControlClearProjectPolicyInput,
  ) {
    const operation = "clear-project-policy" as const;
    const input = yield* decodeClearProjectPolicyInput(rawInput).pipe(
      Effect.mapError(() => validationError(operation)),
    );
    const context = yield* loadResolutionContext(operation);
    yield* repository.clearProjectPolicy(input).pipe(mapRepositoryFailure(operation));
    return {
      appPolicy: context.settings.agentControlPolicy ?? null,
      projectPolicy: null,
      preflight: resolvePolicy({
        context,
        appPolicy: context.settings.agentControlPolicy,
        projectPolicy: undefined,
      }),
    } satisfies AgentControlPolicyStateResult;
  }, catchPolicyDefect("clear-project-policy"));

  const preflightPolicy = Effect.fn("AgentControlPolicyService.preflightPolicy")(function* (
    rawInput: AgentControlPreflightPolicyInput,
  ) {
    const operation = "preflight-policy" as const;
    const input = yield* decodePreflightPolicyInput(rawInput).pipe(
      Effect.mapError(() => validationError(operation)),
    );
    yield* repository.ensureProjectAvailable(input.projectId).pipe(mapRepositoryFailure(operation));
    const persistedProjectPolicy =
      input.projectPolicy === undefined
        ? Option.getOrUndefined(
            yield* repository
              .getProjectPolicy(input.projectId)
              .pipe(mapRepositoryFailure(operation)),
          )?.policy
        : undefined;
    const context = yield* loadResolutionContext(operation);
    return resolvePolicy({
      context,
      appPolicy:
        input.appPolicy === undefined
          ? context.settings.agentControlPolicy
          : (input.appPolicy ?? undefined),
      projectPolicy:
        input.projectPolicy === undefined
          ? persistedProjectPolicy
          : (input.projectPolicy ?? undefined),
    });
  }, catchPolicyDefect("preflight-policy"));

  const preflightRuntime = Effect.fn("AgentControlPolicyService.preflightRuntime")(function* (
    rawInput: AgentControlPreflightRuntimeInput,
  ) {
    const operation = "preflight-runtime" as const;
    const input = yield* decodePreflightRuntimeInput(rawInput).pipe(
      Effect.mapError(() => validationError(operation)),
    );
    yield* repository.ensureProjectAvailable(input.projectId).pipe(mapRepositoryFailure(operation));
    const persistedProjectPolicy =
      input.projectPolicy === undefined
        ? Option.getOrUndefined(
            yield* repository
              .getProjectPolicy(input.projectId)
              .pipe(mapRepositoryFailure(operation)),
          )?.policy
        : undefined;
    const context = yield* loadResolutionContext(operation);
    const resolution = resolvePolicyResolution({
      context,
      appPolicy:
        input.appPolicy === undefined
          ? context.settings.agentControlPolicy
          : (input.appPolicy ?? undefined),
      projectPolicy:
        input.projectPolicy === undefined
          ? persistedProjectPolicy
          : (input.projectPolicy ?? undefined),
    });
    const staticPreflight = preflightResult(resolution, context.providerInstances);
    const candidatesByRole = runtimeEligibleCandidates(resolution);
    if (!canProbeRuntimeCandidates(resolution, candidatesByRole)) {
      return staticFailureRuntimeResult(staticPreflight);
    }

    const instanceIds: Array<ProviderInstanceId> = [];
    const seenInstanceIds = new Set<ProviderInstanceId>();
    for (const role of AGENT_CONTROL_ROLES) {
      for (const { candidate } of candidatesByRole.get(role) ?? []) {
        if (seenInstanceIds.has(candidate.selection.instanceId)) continue;
        seenInstanceIds.add(candidate.selection.instanceId);
        instanceIds.push(candidate.selection.instanceId);
      }
    }

    const observations = new Map(
      yield* Effect.forEach(
        instanceIds,
        (instanceId) =>
          probeRuntimeInstance(instanceId, context).pipe(
            Effect.map((observation) => [instanceId, observation] as const),
          ),
        { concurrency: AGENT_CONTROL_RUNTIME_PROBE_CONCURRENCY },
      ),
    );
    const allowlist = resolution.policy.providerAllowlist
      ? new Set(resolution.policy.providerAllowlist)
      : undefined;
    const roles = AGENT_CONTROL_ROLES.map((role) => {
      const route = resolution.policy.roleRoutes[role];
      const candidates = (candidatesByRole.get(role) ?? []).map(({ candidate }, candidateIndex) =>
        projectRuntimeCandidate({
          candidateIndex,
          candidate,
          expectedDriverKind: route.driverKind,
          allowlist,
          observation:
            observations.get(candidate.selection.instanceId) ??
            ({
              driverKind:
                context.providerInstances.get(candidate.selection.instanceId)?.driverKind ?? null,
              snapshot: null,
              errorCode: "provider-instance-missing",
            } satisfies RuntimeProbeObservation),
        }),
      );
      const selectedCandidate = candidates.find((candidate) => candidate.runtimeReady);
      return {
        role,
        accessMode: route.accessMode,
        strict: route.strict,
        candidates,
        selectedCandidateIndex: selectedCandidate?.candidateIndex ?? null,
        errorCode: selectedCandidate === undefined ? ("role-runtime-unresolved" as const) : null,
      };
    });

    return {
      ok: roles.every((role) => role.selectedCandidateIndex !== null),
      staticPreflight,
      roles,
    } satisfies AgentControlPreflightRuntimeResult;
  }, catchPolicyDefect("preflight-runtime"));

  return AgentControlPolicyService.of({
    getPolicy,
    setProjectPolicy,
    clearProjectPolicy,
    preflightPolicy,
    preflightRuntime,
  });
});

export const AgentControlPolicyServiceLive = Layer.effect(
  AgentControlPolicyService,
  makeAgentControlPolicyService,
);
