import { AgentControlTaskId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlStageRunLeaseId } from "./identity.ts";

it.effect("derives a deterministic length-framed lease scope per project and task", () =>
  Effect.gen(function* () {
    const first = yield* deriveAgentControlStageRunLeaseId({
      projectId: ProjectId.make("project-a"),
      taskId: AgentControlTaskId.make("task-a"),
    });
    const repeated = yield* deriveAgentControlStageRunLeaseId({
      projectId: ProjectId.make("project-a"),
      taskId: AgentControlTaskId.make("task-a"),
    });
    const otherTask = yield* deriveAgentControlStageRunLeaseId({
      projectId: ProjectId.make("project-a"),
      taskId: AgentControlTaskId.make("task-b"),
    });
    const otherProject = yield* deriveAgentControlStageRunLeaseId({
      projectId: ProjectId.make("project-b"),
      taskId: AgentControlTaskId.make("task-a"),
    });

    assert.equal(first, repeated);
    assert.match(first, /^stage-run-lease-[0-9a-f]{64}$/);
    assert.notEqual(first, otherTask);
    assert.notEqual(first, otherProject);
  }),
);
