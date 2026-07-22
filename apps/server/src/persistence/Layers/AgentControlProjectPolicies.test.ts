import { ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { resolveAgentControlPolicy } from "@t3tools/shared/agentControl";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlProjectPolicyRepository } from "../Services/AgentControlProjectPolicies.ts";
import { AgentControlProjectPolicyRepositoryLive } from "./AgentControlProjectPolicies.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const repositoryLayer = it.layer(
  AgentControlProjectPolicyRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

const insertProject = Effect.fn("insertAgentControlPolicyTestProject")(function* (
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

repositoryLayer("AgentControlProjectPolicyRepository", (it) => {
  it.effect("round-trips partial policies without checking provider availability", () =>
    Effect.gen(function* () {
      const policies = yield* AgentControlProjectPolicyRepository;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("agent-control-policy-roundtrip");
      const unavailableProvider = ProviderInstanceId.make("temporarily-unavailable");
      yield* insertProject(projectId);

      const created = yield* policies.setProjectPolicy({
        projectId,
        expectedRevision: 0,
        policy: {
          providerAllowlist: [unavailableProvider],
          roleRoutes: {
            reviewer: {
              candidates: [{ instanceId: unavailableProvider, model: "future-model" }],
              strict: true,
            },
          },
        },
      });

      assert.strictEqual(created.revision, 1);
      assert.match(created.updatedAt, /^\d{4}-\d{2}-\d{2}T/);
      assert.deepStrictEqual(
        Option.getOrThrow(yield* policies.getProjectPolicy(projectId)),
        created,
      );

      const resolution = resolveAgentControlPolicy({
        defaults: {
          defaultFallbacks: [{ instanceId: unavailableProvider, model: "future-model" }],
        },
        projectPolicy: created.policy,
        providerInstances: new Map(),
      });
      assert.isFalse(resolution.ok);
      if (resolution.ok) {
        return yield* Effect.die("Expected the unavailable provider to fail during resolution.");
      }
      assert.isTrue(
        resolution.errors.some(
          (error) =>
            error.code === "provider-not-configured" &&
            error.role === "reviewer" &&
            error.instanceId === unavailableProvider,
        ),
      );

      const rows = yield* sql<{ readonly policyJson: string }>`
        SELECT policy_json AS "policyJson"
        FROM agent_control_project_policies
        WHERE project_id = ${projectId}
      `;
      assert.deepStrictEqual(rows, [
        {
          policyJson:
            '{"providerAllowlist":["temporarily-unavailable"],"roleRoutes":{"reviewer":{"candidates":[{"instanceId":"temporarily-unavailable","model":"future-model"}],"strict":true}}}',
        },
      ]);

      const updated = yield* policies.setProjectPolicy({
        projectId,
        expectedRevision: created.revision,
        policy: { fullAccess: true },
      });
      assert.strictEqual(updated.revision, 2);
      assert.deepStrictEqual(updated.policy, { fullAccess: true });
    }),
  );

  it.effect("fails an optimistic update when the expected revision is stale", () =>
    Effect.gen(function* () {
      const policies = yield* AgentControlProjectPolicyRepository;
      const projectId = ProjectId.make("agent-control-policy-conflict");
      yield* insertProject(projectId);
      yield* policies.setProjectPolicy({
        projectId,
        expectedRevision: 0,
        policy: { fullAccess: false },
      });

      const error = yield* Effect.flip(
        policies.setProjectPolicy({
          projectId,
          expectedRevision: 0,
          policy: { fullAccess: true },
        }),
      );
      assert.deepInclude(error, {
        _tag: "AgentControlProjectPolicyConflictError",
        projectId,
        expectedRevision: 0,
        actualRevision: 1,
      });
    }),
  );

  it.effect("rejects structurally invalid policies before persistence", () =>
    Effect.gen(function* () {
      const policies = yield* AgentControlProjectPolicyRepository;
      const projectId = ProjectId.make("agent-control-policy-invalid-input");
      yield* insertProject(projectId);

      const error = yield* Effect.flip(
        policies.setProjectPolicy({
          projectId,
          expectedRevision: 0,
          policy: {
            roleRoutes: {
              reviewer: {
                candidates: [],
                strict: true,
              },
            },
          },
        } as never),
      );
      assert.strictEqual(error._tag, "AgentControlProjectPolicyValidationError");
      assert.isTrue(Option.isNone(yield* policies.getProjectPolicy(projectId)));
    }),
  );

  it.effect("quarantines one corrupt row while other project policies remain available", () =>
    Effect.gen(function* () {
      const policies = yield* AgentControlProjectPolicyRepository;
      const sql = yield* SqlClient.SqlClient;
      const corruptProjectId = ProjectId.make("agent-control-policy-corrupt");
      const healthyProjectId = ProjectId.make("agent-control-policy-healthy");
      yield* insertProject(corruptProjectId);
      yield* insertProject(healthyProjectId);
      yield* policies.setProjectPolicy({
        projectId: corruptProjectId,
        expectedRevision: 0,
        policy: { fullAccess: true },
      });
      const healthy = yield* policies.setProjectPolicy({
        projectId: healthyProjectId,
        expectedRevision: 0,
        policy: { roleRoutes: {} },
      });
      yield* sql`
        UPDATE agent_control_project_policies
        SET policy_json = '{"fullAccess":"not-a-boolean"}'
        WHERE project_id = ${corruptProjectId}
      `;

      const error = yield* Effect.flip(policies.getProjectPolicy(corruptProjectId));
      assert.deepInclude(error, {
        _tag: "AgentControlProjectPolicyCorruptError",
        projectId: corruptProjectId,
      });
      assert.deepStrictEqual(
        Option.getOrThrow(yield* policies.getProjectPolicy(healthyProjectId)),
        healthy,
      );

      yield* policies.deleteProjectPolicy(healthyProjectId);
      assert.isTrue(Option.isNone(yield* policies.getProjectPolicy(healthyProjectId)));
    }),
  );
});
