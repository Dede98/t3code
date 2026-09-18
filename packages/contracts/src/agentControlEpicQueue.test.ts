import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  AgentControlEpicQueue,
  AgentControlEpicQueueChangeInput,
} from "./agentControlEpicQueue.ts";

const decodeQueue = Schema.decodeUnknownSync(AgentControlEpicQueue);
const decodeChange = Schema.decodeUnknownSync(AgentControlEpicQueueChangeInput);
const plan = {
  version: 1,
  rationale: "Separate modules; task B consumes task A only after human merge.",
  epics: [
    { issueNodeId: "epic-a", sourceFingerprint: "scope-a" },
    { issueNodeId: "epic-b", sourceFingerprint: "scope-b" },
  ],
  tasks: [
    { issueNodeId: "task-a", dependsOn: [] },
    { issueNodeId: "task-b", dependsOn: ["task-a"] },
  ],
};
const request = {
  projectId: "project",
  commandId: "configure",
  expectedRevision: 2,
  action: { kind: "configure", maxActiveEpics: 2, projectDependencyPlan: plan },
};

describe("parallel Epic queue wire contracts", () => {
  it("retains the complete reviewed graph and exact frozen scopes across serialization", () => {
    expect(decodeChange(JSON.parse(JSON.stringify(request)))).toEqual(request);
  });

  it("rejects invalid Epic limits and incomplete approval evidence", () => {
    for (const maxActiveEpics of [0, 5, 1.5, NaN, Infinity]) {
      expect(() =>
        decodeChange({ ...request, action: { ...request.action, maxActiveEpics } }),
      ).toThrow();
    }
    for (const projectDependencyPlan of [
      { ...plan, rationale: " " },
      { ...plan, version: 2 },
      { ...plan, epics: [{ issueNodeId: "epic-a" }] },
      { ...plan, tasks: [{ issueNodeId: "task-a" }] },
    ]) {
      expect(() =>
        decodeChange({ ...request, action: { ...request.action, projectDependencyPlan } }),
      ).toThrow();
    }
  });

  it("decodes pre-upgrade serial queues without silently opting into parallelism", () => {
    const queue = {
      projectId: "project",
      revision: 4,
      entries: [],
      nextEntryId: null,
      waitReason: null,
      nextCheckAt: null,
    };
    expect(decodeQueue(queue)).toEqual(queue);
    expect(
      decodeChange({ ...request, action: { kind: "configure", maxActiveEpics: 1 } }).action,
    ).toEqual({ kind: "configure", maxActiveEpics: 1 });
  });
});
