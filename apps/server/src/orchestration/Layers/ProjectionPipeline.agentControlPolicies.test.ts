import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, CorrelationId, EventId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { AgentControlProjectPolicyRepository } from "../../persistence/Services/AgentControlProjectPolicies.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import {
  ORCHESTRATION_PROJECTOR_NAMES,
  OrchestrationProjectionPipelineLive,
} from "./ProjectionPipeline.ts";

const layer = it.layer(
  OrchestrationProjectionPipelineLive.pipe(
    Layer.provideMerge(OrchestrationEventStoreLive),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-agent-control-policy-project-delete-",
      }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

layer("Agent Control project policy cleanup", (it) => {
  it.effect("removes only the deleted project's policy", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const pipeline = yield* OrchestrationProjectionPipeline;
      const policies = yield* AgentControlProjectPolicyRepository;
      const deletedProjectId = ProjectId.make("agent-control-deleted-project");
      const retainedProjectId = ProjectId.make("agent-control-retained-project");
      const createdAt = "2026-07-22T10:00:00.000Z";

      const appendAndProject = (event: Parameters<typeof eventStore.append>[0]) =>
        eventStore
          .append(event)
          .pipe(Effect.flatMap((persisted) => pipeline.projectEvent(persisted)));

      for (const [index, projectId] of [deletedProjectId, retainedProjectId].entries()) {
        yield* appendAndProject({
          type: "project.created",
          eventId: EventId.make(`agent-control-project-created-${index}`),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: createdAt,
          commandId: CommandId.make(`agent-control-project-create-command-${index}`),
          causationEventId: null,
          correlationId: CorrelationId.make(`agent-control-project-create-command-${index}`),
          metadata: {},
          payload: {
            projectId,
            title: `Project ${index}`,
            workspaceRoot: `/tmp/agent-control-project-${index}`,
            defaultModelSelection: null,
            scripts: [],
            createdAt,
            updatedAt: createdAt,
          },
        });
      }

      yield* policies.setProjectPolicy({
        projectId: deletedProjectId,
        expectedRevision: 0,
        policy: { fullAccess: true },
      });
      const retained = yield* policies.setProjectPolicy({
        projectId: retainedProjectId,
        expectedRevision: 0,
        policy: { fullAccess: false },
      });

      yield* appendAndProject({
        type: "project.deleted",
        eventId: EventId.make("agent-control-project-deleted"),
        aggregateKind: "project",
        aggregateId: deletedProjectId,
        occurredAt: "2026-07-22T11:00:00.000Z",
        commandId: CommandId.make("agent-control-project-delete-command"),
        causationEventId: null,
        correlationId: CorrelationId.make("agent-control-project-delete-command"),
        metadata: {},
        payload: {
          projectId: deletedProjectId,
          deletedAt: "2026-07-22T11:00:00.000Z",
        },
      });

      assert.isTrue(Option.isNone(yield* policies.getProjectPolicy(deletedProjectId)));
      assert.deepStrictEqual(
        Option.getOrThrow(yield* policies.getProjectPolicy(retainedProjectId)),
        retained,
      );
    }),
  );

  it.effect("preserves the policy while rebuilding the project projection", () =>
    Effect.gen(function* () {
      const eventStore = yield* OrchestrationEventStore;
      const pipeline = yield* OrchestrationProjectionPipeline;
      const policies = yield* AgentControlProjectPolicyRepository;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("agent-control-rebuilt-project");
      const createdAt = "2026-07-22T10:00:00.000Z";

      const created = yield* eventStore.append({
        type: "project.created",
        eventId: EventId.make("agent-control-rebuild-project-created"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: createdAt,
        commandId: CommandId.make("agent-control-rebuild-project-command"),
        causationEventId: null,
        correlationId: CorrelationId.make("agent-control-rebuild-project-command"),
        metadata: {},
        payload: {
          projectId,
          title: "Rebuilt Project",
          workspaceRoot: "/tmp/agent-control-rebuilt-project",
          defaultModelSelection: null,
          scripts: [],
          createdAt,
          updatedAt: createdAt,
        },
      });
      yield* pipeline.projectEvent(created);
      const policy = yield* policies.setProjectPolicy({
        projectId,
        expectedRevision: 0,
        policy: { fullAccess: true },
      });

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
          DELETE FROM projection_projects
          WHERE project_id = ${projectId}
        `;
          yield* sql`
            DELETE FROM projection_state
            WHERE projector = ${ORCHESTRATION_PROJECTOR_NAMES.projects}
          `;
        }),
      );
      assert.deepStrictEqual(
        Option.getOrThrow(yield* policies.getProjectPolicy(projectId)),
        policy,
      );

      yield* pipeline.bootstrap;

      const projects = yield* sql<{ readonly title: string }>`
        SELECT title
        FROM projection_projects
        WHERE project_id = ${projectId}
      `;
      assert.deepStrictEqual(projects, [{ title: "Rebuilt Project" }]);
      assert.deepStrictEqual(
        Option.getOrThrow(yield* policies.getProjectPolicy(projectId)),
        policy,
      );
    }),
  );
});
