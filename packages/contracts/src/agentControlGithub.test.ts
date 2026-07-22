import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AgentControlGithubPollOnceInput,
  AgentControlGithubRpcError,
  AgentControlGithubSetTrackerConfigInput,
} from "./agentControlGithub.ts";
import { ProjectId } from "./baseSchemas.ts";

const decodeSetTrackerConfig = Schema.decodeUnknownSync(AgentControlGithubSetTrackerConfigInput);
const decodePollOnce = Schema.decodeUnknownSync(AgentControlGithubPollOnceInput);
const encodeRpcError = Schema.encodeUnknownSync(AgentControlGithubRpcError);

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

  it("keeps wire errors closed and free of process or GitHub details", () => {
    const error = new AgentControlGithubRpcError({
      code: "github-timeout",
      operation: "poll-once",
      projectId: ProjectId.make("project-1"),
    });
    const encoded = encodeRpcError(error);
    expect(encoded).toEqual({
      _tag: "AgentControlGithubRpcError",
      code: "github-timeout",
      operation: "poll-once",
      projectId: "project-1",
    });
    expect(JSON.stringify(encoded)).not.toMatch(/argv|cwd|stderr|token|exception|body|title/i);
  });
});
