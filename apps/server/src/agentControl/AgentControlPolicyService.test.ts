import {
  AGENT_CONTROL_ROLES,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type AgentControlAppPolicy,
  type AgentControlProjectPolicy,
  type AgentControlPreflightPolicyResult,
  type AgentControlPreflightRole,
  type AgentControlRole,
  type AgentControlRoleRoute,
  type ModelSelection,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlProjectPolicyRepositoryLive } from "../persistence/Layers/AgentControlProjectPolicies.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { AgentControlProjectPolicyRepository } from "../persistence/Services/AgentControlProjectPolicies.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  AgentControlPolicyService,
  AgentControlPolicyServiceLive,
} from "./AgentControlPolicyService.ts";

const BASE_INSTANCE = ProviderInstanceId.make("codex-work");
const APP_INSTANCE = ProviderInstanceId.make("claude-work");
const PROJECT_INSTANCE = ProviderInstanceId.make("opencode-work");
const DISABLED_INSTANCE = ProviderInstanceId.make("codex-disabled");
const MISSING_INSTANCE = ProviderInstanceId.make("missing-provider");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");

const BASE_SELECTION = selection(BASE_INSTANCE, "baseline-model");
const APP_SELECTION = selection(APP_INSTANCE, "app-model");
const PROJECT_SELECTION = selection(PROJECT_INSTANCE, "project-model");

type ProviderSpec = {
  readonly instanceId: ProviderInstanceId;
  readonly driverKind: ProviderDriverKind;
  readonly enabled: boolean;
};

const DEFAULT_PROVIDERS: ReadonlyArray<ProviderSpec> = [
  { instanceId: BASE_INSTANCE, driverKind: CODEX_DRIVER, enabled: true },
  { instanceId: APP_INSTANCE, driverKind: CLAUDE_DRIVER, enabled: true },
  { instanceId: PROJECT_INSTANCE, driverKind: OPENCODE_DRIVER, enabled: true },
  { instanceId: DISABLED_INSTANCE, driverKind: CODEX_DRIVER, enabled: false },
];

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

function providerInstance(spec: ProviderSpec): ProviderInstance {
  return {
    ...spec,
    displayName: undefined,
    continuationIdentity: {
      driverKind: spec.driverKind,
      continuationKey: `${spec.driverKind}:instance:${spec.instanceId}`,
    },
    snapshot: {} as ProviderInstance["snapshot"],
    adapter: {} as ProviderInstance["adapter"],
    textGeneration: {} as ProviderInstance["textGeneration"],
  };
}

function makePolicyLayer(
  options: {
    readonly appPolicy?: AgentControlAppPolicy;
    readonly baseline?: ModelSelection;
    readonly providers?: ReadonlyArray<ProviderSpec>;
    readonly providerDefect?: unknown;
  } = {},
) {
  const repositoryLayer = AgentControlProjectPolicyRepositoryLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
  );
  const settingsLayer = ServerSettings.ServerSettingsService.layerTest({
    textGenerationModelSelection: options.baseline ?? BASE_SELECTION,
    ...(options.appPolicy === undefined ? {} : { agentControlPolicy: options.appPolicy }),
  });
  const providerLayer = Layer.mock(ProviderInstanceRegistry)({
    listInstances:
      options.providerDefect === undefined
        ? Effect.succeed((options.providers ?? DEFAULT_PROVIDERS).map(providerInstance))
        : Effect.die(options.providerDefect),
    listUnavailable: Effect.succeed([]),
  });

  return AgentControlPolicyServiceLive.pipe(
    Layer.provideMerge(repositoryLayer),
    Layer.provide(settingsLayer),
    Layer.provide(providerLayer),
  );
}

const insertProject = Effect.fn("insertAgentControlPolicyServiceTestProject")(function* (
  projectId: ProjectId,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO projection_projects (
      project_id,
      title,
      workspace_root,
      default_model_selection_json,
      scripts_json,
      created_at,
      updated_at,
      deleted_at
    ) VALUES (
      ${projectId},
      ${`Project ${projectId}`},
      ${`/tmp/${projectId}`},
      NULL,
      '[]',
      '2026-07-22T10:00:00.000Z',
      '2026-07-22T10:00:00.000Z',
      NULL
    )
  `;
});

function roleResult(
  preflight: AgentControlPreflightPolicyResult,
  role: AgentControlRole,
): AgentControlPreflightRole {
  const result = preflight.roles.find((entry) => entry.role === role);
  assert.isDefined(result);
  return result;
}

it.effect("AgentControlPolicyService gets baseline policy without overrides", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-baseline");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.getPolicy({ projectId });

    assert.isNull(result.appPolicy);
    assert.isNull(result.projectPolicy);
    assert.isTrue(result.preflight.ok);
    for (const role of result.preflight.roles) {
      assert.deepStrictEqual(role.validCandidates, [
        {
          selection: BASE_SELECTION,
          source: "default-fallback",
          driverKind: CODEX_DRIVER,
        },
      ]);
    }
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService gets configured app and project policies", () => {
  const appPolicy: AgentControlAppPolicy = {
    roleRoutes: { reviewer: route([APP_SELECTION]) },
  };
  return Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-layered-get");
    yield* insertProject(projectId);
    const repository = yield* AgentControlProjectPolicyRepository;
    const service = yield* AgentControlPolicyService;
    const persisted = yield* repository.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: {
        roleRoutes: { implementer: route([PROJECT_SELECTION]) },
        fullAccess: true,
      },
    });

    const result = yield* service.getPolicy({ projectId });

    assert.deepStrictEqual(result.appPolicy, appPolicy);
    assert.deepStrictEqual(result.projectPolicy, persisted);
    assert.deepStrictEqual(roleResult(result.preflight, "reviewer").validCandidates, [
      { selection: APP_SELECTION, source: "role-route", driverKind: CLAUDE_DRIVER },
    ]);
    assert.deepStrictEqual(roleResult(result.preflight, "implementer").validCandidates, [
      { selection: PROJECT_SELECTION, source: "role-route", driverKind: OPENCODE_DRIVER },
    ]);
  }).pipe(Effect.provide(makePolicyLayer({ appPolicy })));
});

it.effect("AgentControlPolicyService setProjectPolicy round-trips with revision", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-set-roundtrip");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    const policy: AgentControlProjectPolicy = { fullAccess: true };

    const setResult = yield* service.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy,
    });
    const getResult = yield* service.getPolicy({ projectId });

    assert.strictEqual(setResult.projectPolicy?.revision, 1);
    assert.deepStrictEqual(setResult.projectPolicy?.policy, policy);
    assert.deepStrictEqual(getResult.projectPolicy, setResult.projectPolicy);
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService rejects a stale set revision", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-stale-set");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    yield* service.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { fullAccess: false },
    });

    const error = yield* Effect.flip(
      service.setProjectPolicy({
        projectId,
        expectedRevision: 0,
        policy: { fullAccess: true },
      }),
    );

    assert.deepInclude(error, {
      code: "revision-conflict",
      projectId,
      expectedRevision: 0,
      actualRevision: 1,
    });
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService returns wire-safe validation and internal errors", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-safe-errors");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const validation = yield* Effect.flip(
      service.setProjectPolicy({
        projectId,
        expectedRevision: -1,
        policy: {},
      } as never),
    );
    assert.strictEqual(validation.code, "validation");
    if (validation.code !== "validation") {
      return yield* Effect.die("Expected validation error");
    }
    assert.strictEqual(validation.operation, "set-project-policy");
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService converts defects to a safe persistence error", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-defect-safe");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const error = yield* Effect.flip(service.getPolicy({ projectId }));

    assert.strictEqual(error.code, "internal-persistence-error");
    if (error.code !== "internal-persistence-error") {
      return yield* Effect.die("Expected internal persistence error");
    }
    assert.strictEqual(error.operation, "get-policy");
    assert.notProperty(error, "cause");
  }).pipe(
    Effect.provide(
      makePolicyLayer({ providerDefect: new Error("secret path /tmp/private.sqlite") }),
    ),
  ),
);

it.effect("AgentControlPolicyService clears only the current revision", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-clear");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    const first = yield* service.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { fullAccess: false },
    });
    const second = yield* service.setProjectPolicy({
      projectId,
      expectedRevision: first.projectPolicy?.revision ?? 0,
      policy: { fullAccess: true },
    });

    const staleError = yield* Effect.flip(
      service.clearProjectPolicy({
        projectId,
        expectedRevision: first.projectPolicy?.revision ?? 0,
      }),
    );
    assert.deepInclude(staleError, {
      code: "revision-conflict",
      expectedRevision: 1,
      actualRevision: 2,
    });

    const cleared = yield* service.clearProjectPolicy({
      projectId,
      expectedRevision: second.projectPolicy?.revision ?? 0,
    });
    assert.isNull(cleared.projectPolicy);
    assert.isNull((yield* service.getPolicy({ projectId })).projectPolicy);
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService distinguishes missing and deleted projects", () =>
  Effect.gen(function* () {
    const missingProjectId = ProjectId.make("policy-service-project-missing");
    const deletedProjectId = ProjectId.make("policy-service-project-deleted");
    yield* insertProject(deletedProjectId);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      UPDATE projection_projects
      SET deleted_at = '2026-07-22T12:00:00.000Z'
      WHERE project_id = ${deletedProjectId}
    `;
    const service = yield* AgentControlPolicyService;

    const missing = yield* Effect.flip(service.getPolicy({ projectId: missingProjectId }));
    const deleted = yield* Effect.flip(service.preflightPolicy({ projectId: deletedProjectId }));

    assert.deepInclude(missing, { code: "project-missing", projectId: missingProjectId });
    assert.deepInclude(deleted, { code: "project-deleted", projectId: deletedProjectId });
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService keeps corrupt persisted policy fail-closed", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-corrupt");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    const sql = yield* SqlClient.SqlClient;
    yield* service.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { fullAccess: true },
    });
    yield* sql`
      UPDATE agent_control_project_policies
      SET policy_json = '{"fullAccess":"invalid"}'
      WHERE project_id = ${projectId}
    `;

    const getError = yield* Effect.flip(service.getPolicy({ projectId }));
    const setError = yield* Effect.flip(
      service.setProjectPolicy({
        projectId,
        expectedRevision: 1,
        policy: { fullAccess: false },
      }),
    );
    const clearError = yield* Effect.flip(
      service.clearProjectPolicy({ projectId, expectedRevision: 1 }),
    );

    assert.strictEqual(getError.code, "policy-corrupt");
    assert.strictEqual(setError.code, "policy-corrupt");
    assert.strictEqual(clearError.code, "policy-corrupt");
    const rows = yield* sql<{ readonly policyJson: string }>`
      SELECT policy_json AS "policyJson"
      FROM agent_control_project_policies
      WHERE project_id = ${projectId}
    `;
    assert.deepStrictEqual(rows, [{ policyJson: '{"fullAccess":"invalid"}' }]);
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService preflight drafts never mutate persistence", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-draft-readonly");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    const repository = yield* AgentControlProjectPolicyRepository;
    const persisted = yield* repository.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { roleRoutes: { reviewer: route([APP_SELECTION]) } },
    });

    const draft = yield* service.preflightPolicy({
      projectId,
      projectPolicy: {
        roleRoutes: { reviewer: route([PROJECT_SELECTION]) },
      },
    });

    assert.deepStrictEqual(roleResult(draft, "reviewer").validCandidates, [
      { selection: PROJECT_SELECTION, source: "role-route", driverKind: OPENCODE_DRIVER },
    ]);
    assert.deepStrictEqual(
      Option.getOrThrow(yield* repository.getProjectPolicy(projectId)),
      persisted,
    );
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService resolves project, app, and baseline hierarchy", () => {
  const appPolicy: AgentControlAppPolicy = { defaultFallbacks: [APP_SELECTION] };
  return Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-hierarchy");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    yield* service.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { defaultFallbacks: [PROJECT_SELECTION] },
    });

    const persisted = yield* service.preflightPolicy({ projectId });
    const withoutProject = yield* service.preflightPolicy({ projectId, projectPolicy: null });
    const withoutOverrides = yield* service.preflightPolicy({
      projectId,
      appPolicy: null,
      projectPolicy: null,
    });

    assert.deepStrictEqual(
      roleResult(persisted, "planner").validCandidates[0]?.selection,
      PROJECT_SELECTION,
    );
    assert.deepStrictEqual(
      roleResult(withoutProject, "planner").validCandidates[0]?.selection,
      APP_SELECTION,
    );
    assert.deepStrictEqual(
      roleResult(withoutOverrides, "planner").validCandidates[0]?.selection,
      BASE_SELECTION,
    );
  }).pipe(Effect.provide(makePolicyLayer({ appPolicy })));
});

it.effect("AgentControlPolicyService does not append fallback to strict routes", () => {
  const appPolicy: AgentControlAppPolicy = {
    roleRoutes: { reviewer: route([APP_SELECTION], { strict: true }) },
  };
  return Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-strict");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({ projectId });

    assert.deepStrictEqual(roleResult(result, "reviewer").validCandidates, [
      { selection: APP_SELECTION, source: "role-route", driverKind: CLAUDE_DRIVER },
    ]);
  }).pipe(Effect.provide(makePolicyLayer({ appPolicy })));
});

it.effect("AgentControlPolicyService reports allowlist failures", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-allowlist");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({
      projectId,
      projectPolicy: {
        providerAllowlist: [BASE_INSTANCE],
        roleRoutes: { reviewer: route([APP_SELECTION]) },
      },
    });

    assert.isFalse(result.ok);
    if (result.ok) return yield* Effect.die("Expected allowlist preflight failure");
    assert.isTrue(
      result.errors.some(
        (error) =>
          error.code === "provider-not-allowed" &&
          error.role === "reviewer" &&
          error.instanceId === APP_INSTANCE,
      ),
    );
    assert.deepStrictEqual(roleResult(result, "reviewer").validCandidates, []);
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService reports disabled providers", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-disabled");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({
      projectId,
      projectPolicy: {
        roleRoutes: {
          repair: route([selection(DISABLED_INSTANCE, "disabled-model")]),
        },
      },
    });

    assert.isFalse(result.ok);
    if (result.ok) return yield* Effect.die("Expected disabled-provider preflight failure");
    assert.isTrue(
      result.errors.some((error) => error.code === "provider-disabled" && error.role === "repair"),
    );
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService reports missing providers", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-provider-missing");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({
      projectId,
      projectPolicy: {
        roleRoutes: { verifier: route([selection(MISSING_INSTANCE, "missing-model")]) },
      },
    });

    assert.isFalse(result.ok);
    if (result.ok) return yield* Effect.die("Expected missing-provider preflight failure");
    assert.isTrue(
      result.errors.some(
        (error) => error.code === "provider-not-configured" && error.role === "verifier",
      ),
    );
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService reports driver kind mismatches", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-driver-mismatch");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({
      projectId,
      projectPolicy: {
        roleRoutes: {
          reviewer: route([BASE_SELECTION], { driverKind: CLAUDE_DRIVER }),
        },
      },
    });

    assert.isFalse(result.ok);
    if (result.ok) return yield* Effect.die("Expected driver-kind preflight failure");
    assert.isTrue(
      result.errors.some(
        (error) =>
          error.code === "driver-kind-mismatch" &&
          error.role === "reviewer" &&
          error.expectedDriverKind === CLAUDE_DRIVER &&
          error.actualDriverKind === CODEX_DRIVER,
      ),
    );
  }).pipe(Effect.provide(makePolicyLayer())),
);

it.effect("AgentControlPolicyService grants full access only to implementer and repair", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("policy-service-full-access");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightPolicy({
      projectId,
      projectPolicy: { fullAccess: true },
    });

    for (const role of AGENT_CONTROL_ROLES) {
      assert.strictEqual(
        roleResult(result, role).accessMode,
        role === "implementer" || role === "repair" ? "full-access" : "restricted",
      );
    }
  }).pipe(Effect.provide(makePolicyLayer())),
);
