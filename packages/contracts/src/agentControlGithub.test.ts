import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AgentControlGithubPollOnceInput,
  AgentControlGithubReactorStatus,
  AgentControlGithubRpcError,
  AgentControlGithubSetTrackerConfigInput,
} from "./agentControlGithub.ts";
import { ProjectId } from "./baseSchemas.ts";

const decodeSetTrackerConfig = Schema.decodeUnknownSync(AgentControlGithubSetTrackerConfigInput);
const decodePollOnce = Schema.decodeUnknownSync(AgentControlGithubPollOnceInput);
const encodeRpcError = Schema.encodeUnknownSync(AgentControlGithubRpcError);
const decodeReactorStatus = Schema.decodeUnknownSync(AgentControlGithubReactorStatus);

describe("Agent Control GitHub contracts", () => {
  it("applies safe tracker defaults and bounded polling", () => {
    const decoded = decodeSetTrackerConfig({
      commandId: "command-1",
      projectId: ProjectId.make("project-1"),
      expectedRevision: 0,
      trackerKind: "github",
      trustedLogins: ["trusted"],
    });
    expect(decoded).toMatchObject({
      readyLabel: "agent:ready",
      pausedLabel: "agent:paused",
      pollIntervalSeconds: 60,
    });
    expect(() =>
      decodeSetTrackerConfig({
        ...decoded,
        pollIntervalSeconds: 1,
      }),
    ).toThrow();
  });

  it("does not expose authority or workspace selection on commands", () => {
    expect(() =>
      decodePollOnce(
        {
          commandId: "command-1",
          projectId: "project-1",
          expectedRevision: 0,
          authority: "human",
          cwd: "/attacker/chosen",
        },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });

  it.each(["github-timeout", "github-issues-disabled"] as const)(
    "keeps %s wire errors closed and free of process or GitHub details",
    (code) => {
      const error = new AgentControlGithubRpcError({
        code,
        operation: "poll-once",
        projectId: ProjectId.make("project-1"),
      });
      const encoded = encodeRpcError(error);
      expect(encoded).toEqual({
        _tag: "AgentControlGithubRpcError",
        code,
        operation: "poll-once",
        projectId: "project-1",
      });
      expect(JSON.stringify(encoded)).not.toMatch(/argv|cwd|stderr|token|exception|body|title/i);
    },
  );

  it("keeps reactor status transport-safe and rejects diagnostic payloads", () => {
    const status = decodeReactorStatus({
      projectId: "project-1",
      activity: "suspended",
      health: "healthy",
      workerStatus: "stopped",
      subscriptionHealth: "healthy",
      circuitState: "open",
      consecutiveFailures: 5,
      lastAttemptAt: "2026-07-23T08:00:00.000Z",
      nextAttemptAt: null,
      reasonCode: "timeline-incomplete",
    });
    expect(status).toEqual({
      projectId: "project-1",
      activity: "suspended",
      health: "healthy",
      workerStatus: "stopped",
      subscriptionHealth: "healthy",
      circuitState: "open",
      consecutiveFailures: 5,
      lastAttemptAt: "2026-07-23T08:00:00.000Z",
      nextAttemptAt: null,
      reasonCode: "timeline-incomplete",
    });
    expect(() =>
      decodeReactorStatus(
        { ...status, stderr: "secret", issueTitle: "untrusted" },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });
});
