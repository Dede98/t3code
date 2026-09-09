import {
  AGENT_CONTROL_ROLES,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type AgentControlAppPolicy,
  type AgentControlPreflightRuntimeResult,
  type AgentControlProjectPolicy,
  type AgentControlRole,
  type AgentControlRoleRoute,
  type ModelSelection,
  type ServerProvider,
  type ServerProviderAuthStatus,
  type ServerProviderState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlProjectPolicyRepositoryLive } from "../persistence/Layers/AgentControlProjectPolicies.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { AgentControlProjectPolicyRepository } from "../persistence/Services/AgentControlProjectPolicies.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  AGENT_CONTROL_RUNTIME_PROBE_TIMEOUT_MS,
  AgentControlPolicyService,
  AgentControlPolicyServiceLive,
} from "./AgentControlPolicyService.ts";

const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const UNKNOWN_DRIVER = ProviderDriverKind.make("forkDriver");
const BASE_INSTANCE = ProviderInstanceId.make("runtime-base");
const FIRST_INSTANCE = ProviderInstanceId.make("runtime-first");
const SECOND_INSTANCE = ProviderInstanceId.make("runtime-second");
const UNKNOWN_INSTANCE = ProviderInstanceId.make("runtime-unknown");
const BASE_MODEL = "runtime-model";
const CHECKED_AT = "2026-07-22T12:00:00.000Z";

function selection(instanceId: ProviderInstanceId, model = BASE_MODEL): ModelSelection {
  return { instanceId, model };
}

function route(
  candidates: ReadonlyArray<ModelSelection>,
  options: { readonly strict?: boolean; readonly driverKind?: ProviderDriverKind } = {},
): AgentControlRoleRoute {
  return {
    candidates,
    strict: options.strict ?? true,
    ...(options.driverKind === undefined ? {} : { driverKind: options.driverKind }),
  };
}

function providerSnapshot(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driverKind?: ProviderDriverKind;
  readonly enabled?: boolean;
  readonly installed?: boolean;
  readonly status?: ServerProviderState;
  readonly authStatus?: ServerProviderAuthStatus;
  readonly models?: ReadonlyArray<string>;
  readonly message?: string;
  readonly authMetadata?: boolean;
}): ServerProvider {
  const authMetadata = input.authMetadata
    ? {
        type: "secret-token-type",
        label: "secret auth label",
        email: "secret@example.invalid",
      }
    : {};
  return {
    instanceId: input.instanceId,
    driver: input.driverKind ?? CODEX_DRIVER,
    enabled: input.enabled ?? true,
    installed: input.installed ?? true,
    version: "1.0.0",
    status: input.status ?? "ready",
    auth: { status: input.authStatus ?? "authenticated", ...authMetadata },
    checkedAt: CHECKED_AT,
    ...(input.message === undefined ? {} : { message: input.message }),
    models: (input.models ?? [BASE_MODEL]).map((model) => ({
      slug: model,
      name: model,
      isCustom: false,
      capabilities: null,
    })),
    slashCommands: [],
    skills: [],
  };
}

function providerInstance(input: {
  readonly instanceId: ProviderInstanceId;
  readonly driverKind?: ProviderDriverKind;
  readonly enabled?: boolean;
  readonly snapshot?: ServerProvider;
  readonly refresh?: Effect.Effect<ServerProvider>;
  readonly onProbe?: () => void;
}): ProviderInstance {
  const driverKind = input.driverKind ?? CODEX_DRIVER;
  const snapshot = input.snapshot ?? providerSnapshot({ instanceId: input.instanceId, driverKind });
  const refresh = input.onProbe
    ? Effect.sync(input.onProbe).pipe(Effect.andThen(input.refresh ?? Effect.succeed(snapshot)))
    : (input.refresh ?? Effect.succeed(snapshot));
  return {
    instanceId: input.instanceId,
    driverKind,
    continuationIdentity: {
      driverKind,
      continuationKey: `${driverKind}:instance:${input.instanceId}`,
    },
    displayName: undefined,
    enabled: input.enabled ?? true,
    snapshot: {
      resolveMaintenance: () => Effect.die("Unexpected maintenance resolution"),
      applyUsageLimits: () => Effect.void,
      getSnapshot: Effect.succeed(snapshot),
      refresh,
      streamChanges: Stream.empty,
    },
    adapter: {} as ProviderInstance["adapter"],
    textGeneration: {} as ProviderInstance["textGeneration"],
  };
}

function unavailableSnapshot(
  instanceId: ProviderInstanceId,
  driverKind: ProviderDriverKind,
): ServerProvider {
  return {
    ...providerSnapshot({
      instanceId,
      driverKind,
      enabled: false,
      installed: false,
      status: "error",
      authStatus: "unknown",
      models: [],
    }),
    availability: "unavailable",
    unavailableReason: "Driver is unavailable.",
  };
}

function makeRuntimeLayer(
  options: {
    readonly listedInstances?: ReadonlyArray<ProviderInstance>;
    readonly getInstance?: (
      instanceId: ProviderInstanceId,
    ) => Effect.Effect<ProviderInstance | undefined>;
    readonly unavailable?: ReadonlyArray<ServerProvider>;
    readonly appPolicy?: AgentControlAppPolicy;
    readonly baseline?: ModelSelection;
  } = {},
) {
  const listedInstances = options.listedInstances ?? [
    providerInstance({ instanceId: BASE_INSTANCE }),
  ];
  const unavailable = options.unavailable ?? [];
  const liveById = new Map(listedInstances.map((instance) => [instance.instanceId, instance]));
  const providerInstances = Object.fromEntries([
    ...listedInstances.map((instance) => [
      instance.instanceId,
      { driver: instance.driverKind, enabled: instance.enabled },
    ]),
    ...unavailable.map((snapshot) => [
      snapshot.instanceId,
      { driver: snapshot.driver, enabled: true },
    ]),
  ]);
  const repositoryLayer = AgentControlProjectPolicyRepositoryLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
  );
  const settingsLayer = ServerSettings.ServerSettingsService.layerTest({
    textGenerationModelSelection: options.baseline ?? selection(BASE_INSTANCE),
    providerInstances,
    ...(options.appPolicy === undefined ? {} : { agentControlPolicy: options.appPolicy }),
  });
  const providerLayer = Layer.mock(ProviderInstanceRegistry)({
    getInstance: options.getInstance ?? ((instanceId) => Effect.succeed(liveById.get(instanceId))),
    listInstances: Effect.succeed(listedInstances),
    listUnavailable: Effect.succeed(unavailable),
  });

  return AgentControlPolicyServiceLive.pipe(
    Layer.provideMerge(repositoryLayer),
    Layer.provide(settingsLayer),
    Layer.provide(providerLayer),
  );
}

const insertProject = Effect.fn("insertAgentControlRuntimePreflightTestProject")(function* (
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
      ${CHECKED_AT},
      ${CHECKED_AT},
      NULL
    )
  `;
});

function roleResult(result: AgentControlPreflightRuntimeResult, role: AgentControlRole) {
  const found = result.roles.find((entry) => entry.role === role);
  assert.isDefined(found);
  return found;
}

it.effect("runtime preflight skips every provider probe when static policy is invalid", () => {
  let probeCount = 0;
  const instance = providerInstance({
    instanceId: BASE_INSTANCE,
    onProbe: () => {
      probeCount += 1;
    },
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-static-invalid");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({
      projectId,
      projectPolicy: { providerAllowlist: [] },
    });

    assert.isFalse(result.ok);
    assert.isFalse(result.staticPreflight.ok);
    if (!result.staticPreflight.ok) {
      assert.isTrue(
        result.staticPreflight.errors.some((error) => error.code === "provider-not-allowed"),
      );
    }
    assert.strictEqual(probeCount, 0);
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [instance] })));
});

it.effect("runtime preflight probes each instance once and shares it across roles", () => {
  let probeCount = 0;
  const instance = providerInstance({
    instanceId: BASE_INSTANCE,
    onProbe: () => {
      probeCount += 1;
    },
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-deduplicates-instances");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });

    assert.isTrue(result.ok);
    assert.strictEqual(probeCount, 1);
    assert.deepStrictEqual(
      result.roles.map((role) => [role.role, role.selectedCandidateIndex]),
      AGENT_CONTROL_ROLES.map((role) => [role, 0]),
    );
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [instance] })));
});

it.effect("runtime preflight selects the next ready candidate on a non-strict route", () => {
  const first = providerInstance({
    instanceId: FIRST_INSTANCE,
    snapshot: providerSnapshot({ instanceId: FIRST_INSTANCE, status: "warning" }),
  });
  const fallback = providerInstance({ instanceId: BASE_INSTANCE });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-selects-ready-fallback");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({
      projectId,
      projectPolicy: {
        roleRoutes: { reviewer: route([selection(FIRST_INSTANCE)], { strict: false }) },
      },
    });
    const reviewer = roleResult(result, "reviewer");

    assert.deepStrictEqual(
      reviewer.candidates.map((candidate) => candidate.providerInstanceId),
      [FIRST_INSTANCE, BASE_INSTANCE],
    );
    assert.strictEqual(reviewer.candidates[0]?.errorCode, "provider-not-ready");
    assert.strictEqual(reviewer.selectedCandidateIndex, 1);
    assert.isTrue(result.ok);
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [first, fallback] })));
});

it.effect(
  "runtime preflight filters a statically invalid non-strict candidate without shifting selection",
  () => {
    let invalidProbeCount = 0;
    let readyProbeCount = 0;
    const invalid = providerInstance({
      instanceId: FIRST_INSTANCE,
      enabled: false,
      onProbe: () => {
        invalidProbeCount += 1;
      },
    });
    const ready = providerInstance({
      instanceId: SECOND_INSTANCE,
      onProbe: () => {
        readyProbeCount += 1;
      },
    });
    const fallback = providerInstance({ instanceId: BASE_INSTANCE });
    return Effect.gen(function* () {
      const projectId = ProjectId.make("runtime-filters-invalid-nonstrict-candidate");
      yield* insertProject(projectId);
      const service = yield* AgentControlPolicyService;
      const result = yield* service.preflightRuntime({
        projectId,
        projectPolicy: {
          roleRoutes: {
            reviewer: route([selection(FIRST_INSTANCE), selection(SECOND_INSTANCE)], {
              strict: false,
            }),
          },
        },
      });
      const reviewer = roleResult(result, "reviewer");

      assert.isFalse(result.staticPreflight.ok);
      assert.isTrue(result.ok);
      assert.deepStrictEqual(
        reviewer.candidates.map((candidate) => candidate.providerInstanceId),
        [SECOND_INSTANCE, BASE_INSTANCE],
      );
      assert.strictEqual(reviewer.selectedCandidateIndex, 0);
      assert.strictEqual(
        result.staticPreflight.roles.find((role) => role.role === "reviewer")?.validCandidates[0]
          ?.selection.instanceId,
        SECOND_INSTANCE,
      );
      assert.strictEqual(invalidProbeCount, 0);
      assert.strictEqual(readyProbeCount, 1);
    }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [invalid, ready, fallback] })));
  },
);

it.effect("runtime preflight preserves strict failure for a statically invalid candidate", () => {
  let probeCount = 0;
  const invalid = providerInstance({
    instanceId: FIRST_INSTANCE,
    enabled: false,
    onProbe: () => {
      probeCount += 1;
    },
  });
  const ready = providerInstance({
    instanceId: SECOND_INSTANCE,
    onProbe: () => {
      probeCount += 1;
    },
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-preserves-static-strict-failure");
    yield* insertProject(projectId);
    const result = yield* (yield* AgentControlPolicyService).preflightRuntime({
      projectId,
      projectPolicy: {
        roleRoutes: {
          reviewer: route([selection(FIRST_INSTANCE), selection(SECOND_INSTANCE)], {
            strict: true,
          }),
        },
      },
    });

    assert.isFalse(result.ok);
    assert.isFalse(result.staticPreflight.ok);
    assert.isNull(roleResult(result, "reviewer").selectedCandidateIndex);
    assert.strictEqual(probeCount, 0);
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [invalid, ready] })));
});

it.effect("runtime preflight never adds a fallback to a strict route", () => {
  const first = providerInstance({
    instanceId: FIRST_INSTANCE,
    snapshot: providerSnapshot({ instanceId: FIRST_INSTANCE, status: "warning" }),
  });
  const fallback = providerInstance({ instanceId: BASE_INSTANCE });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-preserves-strict-route");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({
      projectId,
      projectPolicy: {
        roleRoutes: { reviewer: route([selection(FIRST_INSTANCE)], { strict: true }) },
      },
    });
    const reviewer = roleResult(result, "reviewer");

    assert.deepStrictEqual(
      reviewer.candidates.map((candidate) => candidate.providerInstanceId),
      [FIRST_INSTANCE],
    );
    assert.isNull(reviewer.selectedCandidateIndex);
    assert.strictEqual(reviewer.errorCode, "role-runtime-unresolved");
    assert.isFalse(result.ok);
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [first, fallback] })));
});

it.effect("runtime preflight reports a provider instance removed after static resolution", () => {
  const listed = providerInstance({ instanceId: BASE_INSTANCE });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-instance-missing");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });

    assert.strictEqual(
      roleResult(result, "orchestrator").candidates[0]?.errorCode,
      "provider-instance-missing",
    );
  }).pipe(
    Effect.provide(
      makeRuntimeLayer({
        listedInstances: [listed],
        // @effect-diagnostics-next-line effectSucceedWithVoid:off
        getInstance: () => Effect.succeed(undefined),
      }),
    ),
  );
});

it.effect("runtime preflight reports an instance disabled after static resolution", () => {
  let probeCount = 0;
  const listed = providerInstance({ instanceId: BASE_INSTANCE });
  const disabled = providerInstance({
    instanceId: BASE_INSTANCE,
    enabled: false,
    onProbe: () => {
      probeCount += 1;
    },
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-instance-disabled");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });

    assert.strictEqual(
      roleResult(result, "orchestrator").candidates[0]?.errorCode,
      "provider-disabled",
    );
    assert.strictEqual(probeCount, 0);
  }).pipe(
    Effect.provide(
      makeRuntimeLayer({
        listedInstances: [listed],
        getInstance: () => Effect.succeed(disabled),
      }),
    ),
  );
});

it.effect("runtime preflight distinguishes an unavailable driver from a missing instance", () => {
  const unavailable = unavailableSnapshot(UNKNOWN_INSTANCE, UNKNOWN_DRIVER);
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-driver-unavailable");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });
    const candidate = roleResult(result, "orchestrator").candidates[0];

    assert.isTrue(result.staticPreflight.ok);
    assert.strictEqual(candidate?.driverKind, UNKNOWN_DRIVER);
    assert.strictEqual(candidate?.errorCode, "provider-driver-unavailable");
  }).pipe(
    Effect.provide(
      makeRuntimeLayer({
        listedInstances: [],
        unavailable: [unavailable],
        baseline: selection(UNKNOWN_INSTANCE),
      }),
    ),
  );
});

it.effect("runtime preflight rechecks the role driver-kind constraint", () => {
  let probeCount = 0;
  const listed = providerInstance({ instanceId: BASE_INSTANCE, driverKind: CODEX_DRIVER });
  const changed = providerInstance({
    instanceId: BASE_INSTANCE,
    driverKind: CLAUDE_DRIVER,
    onProbe: () => {
      probeCount += 1;
    },
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-driver-kind-mismatch");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({
      projectId,
      projectPolicy: {
        roleRoutes: {
          reviewer: route([selection(BASE_INSTANCE)], { driverKind: CODEX_DRIVER }),
        },
      },
    });

    assert.strictEqual(
      roleResult(result, "reviewer").candidates[0]?.errorCode,
      "driver-kind-mismatch",
    );
    assert.strictEqual(probeCount, 0);
  }).pipe(
    Effect.provide(
      makeRuntimeLayer({
        listedInstances: [listed],
        getInstance: () => Effect.succeed(changed),
      }),
    ),
  );
});

const readinessCases: ReadonlyArray<{
  readonly name: string;
  readonly snapshot: ServerProvider;
  readonly expectedError: string | null;
}> = [
  {
    name: "provider not installed",
    snapshot: providerSnapshot({ instanceId: BASE_INSTANCE, installed: false }),
    expectedError: "provider-not-installed",
  },
  {
    name: "warning status",
    snapshot: providerSnapshot({ instanceId: BASE_INSTANCE, status: "warning" }),
    expectedError: "provider-not-ready",
  },
  {
    name: "error status",
    snapshot: providerSnapshot({ instanceId: BASE_INSTANCE, status: "error" }),
    expectedError: "provider-not-ready",
  },
  {
    name: "unauthenticated",
    snapshot: providerSnapshot({
      instanceId: BASE_INSTANCE,
      status: "error",
      authStatus: "unauthenticated",
    }),
    expectedError: "provider-unauthenticated",
  },
  {
    name: "authenticated and ready",
    snapshot: providerSnapshot({
      instanceId: BASE_INSTANCE,
      status: "ready",
      authStatus: "authenticated",
    }),
    expectedError: null,
  },
  {
    name: "unknown auth and ready",
    snapshot: providerSnapshot({
      instanceId: BASE_INSTANCE,
      status: "ready",
      authStatus: "unknown",
    }),
    expectedError: null,
  },
  {
    name: "unknown auth and warning",
    snapshot: providerSnapshot({
      instanceId: BASE_INSTANCE,
      status: "warning",
      authStatus: "unknown",
    }),
    expectedError: "provider-not-ready",
  },
  {
    name: "model unavailable",
    snapshot: providerSnapshot({
      instanceId: BASE_INSTANCE,
      models: ["different-model"],
    }),
    expectedError: "model-unavailable",
  },
];

for (const readinessCase of readinessCases) {
  it.effect(`runtime preflight handles ${readinessCase.name}`, () => {
    const instance = providerInstance({
      instanceId: BASE_INSTANCE,
      snapshot: readinessCase.snapshot,
    });
    return Effect.gen(function* () {
      const projectId = ProjectId.make(
        `runtime-${readinessCase.name.toLowerCase().replaceAll(" ", "-")}`,
      );
      yield* insertProject(projectId);
      const service = yield* AgentControlPolicyService;

      const result = yield* service.preflightRuntime({ projectId });
      const candidate = roleResult(result, "orchestrator").candidates[0];

      assert.strictEqual(candidate?.errorCode, readinessCase.expectedError);
      assert.strictEqual(candidate?.runtimeReady, readinessCase.expectedError === null);
      assert.strictEqual(candidate?.providerStatus, readinessCase.snapshot.status);
      assert.strictEqual(candidate?.authStatus, readinessCase.snapshot.auth.status);
    }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [instance] })));
  });
}

it.effect("runtime preflight contains a hanging provider without blocking other results", () => {
  const hanging = providerInstance({
    instanceId: FIRST_INSTANCE,
    refresh: Effect.never,
  });
  const ready = providerInstance({ instanceId: BASE_INSTANCE });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-probe-timeout");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;
    const fiber = yield* service
      .preflightRuntime({
        projectId,
        projectPolicy: {
          roleRoutes: { reviewer: route([selection(FIRST_INSTANCE)], { strict: false }) },
        },
      })
      .pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* TestClock.adjust(Duration.millis(AGENT_CONTROL_RUNTIME_PROBE_TIMEOUT_MS));
    const result = yield* Fiber.join(fiber);

    const reviewer = roleResult(result, "reviewer");
    assert.strictEqual(reviewer.candidates[0]?.errorCode, "provider-probe-timeout");
    assert.strictEqual(reviewer.selectedCandidateIndex, 1);
    assert.isTrue(result.ok);
  }).pipe(
    Effect.provide(
      Layer.merge(TestClock.layer(), makeRuntimeLayer({ listedInstances: [hanging, ready] })),
    ),
  );
});

it.effect("runtime preflight converts probe defects to a closed wire-safe error", () => {
  const instance = providerInstance({
    instanceId: BASE_INSTANCE,
    refresh: Effect.die(
      new Error("secret stderr: token=abc command=/private/bin/provider --credential secret"),
    ),
  });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-probe-defect");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });
    // @effect-diagnostics-next-line preferSchemaOverJson:off
    const serialized = JSON.stringify(result);

    assert.strictEqual(
      roleResult(result, "orchestrator").candidates[0]?.errorCode,
      "provider-probe-failed",
    );
    assert.notInclude(serialized, "secret");
    assert.notInclude(serialized, "stderr");
    assert.notInclude(serialized, "command");
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [instance] })));
});

it.effect("runtime preflight preserves role and candidate order", () => {
  const first = providerInstance({ instanceId: FIRST_INSTANCE });
  const second = providerInstance({ instanceId: SECOND_INSTANCE });
  const base = providerInstance({ instanceId: BASE_INSTANCE });
  const appPolicy: AgentControlAppPolicy = {
    roleRoutes: {
      reviewer: route([selection(SECOND_INSTANCE), selection(FIRST_INSTANCE)]),
    },
  };
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-stable-order");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });

    assert.deepStrictEqual(
      result.roles.map((role) => role.role),
      [...AGENT_CONTROL_ROLES],
    );
    assert.deepStrictEqual(
      roleResult(result, "reviewer").candidates.map((candidate) => candidate.providerInstanceId),
      [SECOND_INSTANCE, FIRST_INSTANCE],
    );
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [first, second, base], appPolicy })));
});

it.effect("runtime preflight drafts never mutate persisted policy", () => {
  const base = providerInstance({ instanceId: BASE_INSTANCE });
  const second = providerInstance({ instanceId: SECOND_INSTANCE });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-draft-readonly");
    yield* insertProject(projectId);
    const repository = yield* AgentControlProjectPolicyRepository;
    const service = yield* AgentControlPolicyService;
    const persistedPolicy: AgentControlProjectPolicy = {
      roleRoutes: { reviewer: route([selection(BASE_INSTANCE)]) },
    };
    const persisted = yield* repository.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: persistedPolicy,
    });

    const result = yield* service.preflightRuntime({
      projectId,
      projectPolicy: {
        roleRoutes: { reviewer: route([selection(SECOND_INSTANCE)]) },
      },
    });

    assert.strictEqual(
      roleResult(result, "reviewer").candidates[0]?.providerInstanceId,
      SECOND_INSTANCE,
    );
    assert.deepStrictEqual(
      Option.getOrThrow(yield* repository.getProjectPolicy(projectId)),
      persisted,
    );
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [base, second] })));
});

it.effect("runtime preflight preserves omitted, null, and object draft semantics", () => {
  const base = providerInstance({ instanceId: BASE_INSTANCE });
  const first = providerInstance({ instanceId: FIRST_INSTANCE });
  const second = providerInstance({ instanceId: SECOND_INSTANCE });
  const appPolicy: AgentControlAppPolicy = { defaultFallbacks: [selection(SECOND_INSTANCE)] };
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-draft-semantics");
    yield* insertProject(projectId);
    const repository = yield* AgentControlProjectPolicyRepository;
    const service = yield* AgentControlPolicyService;
    yield* repository.setProjectPolicy({
      projectId,
      expectedRevision: 0,
      policy: { defaultFallbacks: [selection(FIRST_INSTANCE)] },
    });

    const persisted = yield* service.preflightRuntime({ projectId });
    const withoutProject = yield* service.preflightRuntime({ projectId, projectPolicy: null });
    const withoutOverrides = yield* service.preflightRuntime({
      projectId,
      appPolicy: null,
      projectPolicy: null,
    });

    assert.strictEqual(
      roleResult(persisted, "planner").candidates[0]?.providerInstanceId,
      FIRST_INSTANCE,
    );
    assert.strictEqual(
      roleResult(withoutProject, "planner").candidates[0]?.providerInstanceId,
      SECOND_INSTANCE,
    );
    assert.strictEqual(
      roleResult(withoutOverrides, "planner").candidates[0]?.providerInstanceId,
      BASE_INSTANCE,
    );
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [base, first, second], appPolicy })));
});

it.effect("runtime preflight returns typed validation and project errors", () =>
  Effect.gen(function* () {
    const service = yield* AgentControlPolicyService;
    const missingProjectId = ProjectId.make("runtime-missing-project");

    const validation = yield* Effect.flip(service.preflightRuntime({ projectId: "" } as never));
    const missing = yield* Effect.flip(service.preflightRuntime({ projectId: missingProjectId }));

    assert.deepInclude(validation, {
      code: "validation",
      operation: "preflight-runtime",
    });
    assert.deepInclude(missing, {
      code: "project-missing",
      projectId: missingProjectId,
    });
  }).pipe(Effect.provide(makeRuntimeLayer())),
);

it.effect("runtime preflight never serializes provider messages or auth metadata", () => {
  const snapshot = providerSnapshot({
    instanceId: BASE_INSTANCE,
    message: "secret stderr from /private/provider --token abc",
    authMetadata: true,
  });
  const instance = providerInstance({ instanceId: BASE_INSTANCE, snapshot });
  return Effect.gen(function* () {
    const projectId = ProjectId.make("runtime-redacts-provider-details");
    yield* insertProject(projectId);
    const service = yield* AgentControlPolicyService;

    const result = yield* service.preflightRuntime({ projectId });
    // @effect-diagnostics-next-line preferSchemaOverJson:off
    const serialized = JSON.stringify(result);

    assert.isTrue(result.ok);
    assert.notInclude(serialized, "secret");
    assert.notInclude(serialized, "stderr");
    assert.notInclude(serialized, "/private/provider");
    assert.notInclude(serialized, "example.invalid");
  }).pipe(Effect.provide(makeRuntimeLayer({ listedInstances: [instance] })));
});
