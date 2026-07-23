import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AgentControlTaskListResult,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  AgentControlTaskStatus,
} from "./agentControlTask.ts";
import { AgentControlTaskId, ProjectId } from "./baseSchemas.ts";

const decodeReconcile = Schema.decodeUnknownSync(AgentControlTaskReconcileOnceInput);
const decodeList = Schema.decodeUnknownSync(AgentControlTaskListResult);
const decodeStatus = Schema.decodeUnknownSync(AgentControlTaskStatus);
const encodeError = Schema.encodeUnknownSync(AgentControlTaskRpcError);

describe("Agent Control task contracts", () => {
  it("allows future execution statuses while keeping reconcile input controller-owned", () => {
    for (const status of [
      "candidate",
      "needs-attention",
      "cancelled",
      "queued",
      "running",
      "waiting",
      "succeeded",
      "failed",
    ]) {
      expect(decodeStatus(status)).toBe(status);
    }
    expect(() =>
      decodeReconcile(
        {
          projectId: "project-1",
          taskId: "attacker-task",
          authority: "human",
          commandId: "attacker-command",
          status: "running",
          stage: "implementation",
        },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });

  it("keeps list results body-free and marks source text untrusted", () => {
    const result = decodeList({
      projectId: ProjectId.make("project-1"),
      quarantinedCount: 0,
      tasks: [
        {
          schemaVersion: 1,
          taskId: AgentControlTaskId.make("task-1"),
          source: {
            projectId: ProjectId.make("project-1"),
            repositoryNodeId: "repo-1",
            issueNodeId: "issue-1",
            issueNumber: 1,
            issueUrl: "https://github.test/o/r/issues/1",
          },
          status: "candidate",
          sourceGate: "eligible",
          stage: "intake",
          sourceUpdatedAt: "2026-07-23T10:00:00.000Z",
          githubIntakeSequence: 1,
          title: "untrusted",
          contentTrust: "untrusted-external",
          createdAt: "2026-07-23T10:00:00.000Z",
          updatedAt: "2026-07-23T10:00:00.000Z",
          revision: 1,
          sequence: 1,
        },
      ],
    });
    expect(JSON.stringify(result)).not.toContain("body");
    expect(result.tasks[0]?.contentTrust).toBe("untrusted-external");
  });

  it("keeps wire errors closed", () => {
    const encoded = encodeError(
      new AgentControlTaskRpcError({
        code: "source-identity-conflict",
        operation: "reconcile-once",
        projectId: ProjectId.make("project-1"),
        taskId: AgentControlTaskId.make("task-1"),
      }),
    );
    expect(encoded).toEqual({
      _tag: "AgentControlTaskRpcError",
      code: "source-identity-conflict",
      operation: "reconcile-once",
      projectId: "project-1",
      taskId: "task-1",
    });
    expect(JSON.stringify(encoded)).not.toMatch(
      /body|title|githubRaw|command|path|exception|credential|token/i,
    );
  });
});
