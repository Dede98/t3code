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
  AgentControlSetProjectPolicyInput,
  type AgentControlAppPolicy,
  type AgentControlPreflightError,
  type AgentControlProjectPolicy,
  type ProviderInstanceId,
  type ServerSettings,
} from "@t3tools/contracts";
import {
  resolveAgentControlPolicy,
  type AgentControlConfiguredProviderInstance,
  type AgentControlPolicyResolution,
} from "@t3tools/shared/agentControl";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
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
  | "preflight-policy";

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
};

const decodeGetPolicyInput = Schema.decodeUnknownEffect(AgentControlGetPolicyInput);
const decodeSetProjectPolicyInput = Schema.decodeUnknownEffect(AgentControlSetProjectPolicyInput);
const decodeClearProjectPolicyInput = Schema.decodeUnknownEffect(
  AgentControlClearProjectPolicyInput,
);
const decodePreflightPolicyInput = Schema.decodeUnknownEffect(AgentControlPreflightPolicyInput);

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

function resolvePolicy(input: {
  readonly context: ResolutionContext;
  readonly appPolicy: AgentControlAppPolicy | undefined;
  readonly projectPolicy: AgentControlProjectPolicy | undefined;
}): AgentControlPreflightPolicyResult {
  return preflightResult(
    resolveAgentControlPolicy({
      defaults: {
        defaultFallbacks: [input.context.settings.textGenerationModelSelection],
      },
      ...(input.appPolicy === undefined ? {} : { appPolicy: input.appPolicy }),
      ...(input.projectPolicy === undefined ? {} : { projectPolicy: input.projectPolicy }),
      providerInstances: input.context.providerInstances,
    }),
    input.context.providerInstances,
  );
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
      const [liveInstances, unavailableInstances] = yield* Effect.all([
        providerRegistry.listInstances,
        providerRegistry.listUnavailable,
      ]);
      const providerInstances = new Map<
        ProviderInstanceId,
        AgentControlConfiguredProviderInstance
      >();
      for (const instance of liveInstances) {
        providerInstances.set(instance.instanceId, {
          driverKind: instance.driverKind,
          enabled: instance.enabled,
        });
      }
      for (const instance of unavailableInstances) {
        if (!providerInstances.has(instance.instanceId)) {
          providerInstances.set(instance.instanceId, {
            driverKind: instance.driver,
            enabled: instance.enabled,
          });
        }
      }
      return { settings, providerInstances } satisfies ResolutionContext;
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

  return AgentControlPolicyService.of({
    getPolicy,
    setProjectPolicy,
    clearProjectPolicy,
    preflightPolicy,
  });
});

export const AgentControlPolicyServiceLive = Layer.effect(
  AgentControlPolicyService,
  makeAgentControlPolicyService,
);
