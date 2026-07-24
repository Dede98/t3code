import { AgentControlTaskId, type AgentControlTaskState, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlAttemptId,
  deriveAgentControlSourceIdentityFingerprint,
  deriveAgentControlStageRunId,
} from "./identity.ts";

const projectId = ProjectId.make("stage-run-identity-project");
const taskId = AgentControlTaskId.make("stage-run-identity-task");
const task = (body: string | null): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId,
  source: {
    projectId,
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    issueNumber: 17,
    issueUrl: "https://example.test/issues/17",
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: "2026-07-24T10:00:00.000Z",
  githubIntakeSequence: 5,
  sourceSnapshot: {
    repositoryNodeId: "repository-node",
    issueNodeId: "issue-node",
    number: 17,
    url: "https://example.test/issues/17",
    state: "open",
    title: "untrusted title",
    body,
    contentTrust: "untrusted-external",
    updatedAt: "2026-07-24T10:00:00.000Z",
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt: "2026-07-24T10:00:00.000Z",
  updatedAt: "2026-07-24T10:00:00.000Z",
  revision: 3,
  sequence: 4,
});

it.effect("derives collision-safe deterministic stage-run and attempt ids", () =>
  Effect.gen(function* () {
    const input = {
      projectId,
      taskId,
      taskRevision: 3,
      githubIntakeSequence: 5,
      sourceIdentityFingerprint: "a".repeat(64),
      stageKind: "planning" as const,
      stageOrdinal: 1,
    };
    const first = yield* deriveAgentControlStageRunId(input);
    const replay = yield* deriveAgentControlStageRunId(input);
    const otherRevision = yield* deriveAgentControlStageRunId({
      ...input,
      taskRevision: 4,
    });
    const otherFraming = yield* deriveAgentControlStageRunId({
      ...input,
      projectId: ProjectId.make("stage-run-identity-projec"),
      taskId: AgentControlTaskId.make("tstage-run-identity-task"),
    });
    assert.equal(first, replay);
    assert.notEqual(first, otherRevision);
    assert.notEqual(first, otherFraming);
    assert.notEqual(
      first,
      yield* deriveAgentControlStageRunId({
        ...input,
        sourceIdentityFingerprint: "b".repeat(64),
      }),
    );
    assert.equal(
      yield* deriveAgentControlAttemptId(first, 1),
      yield* deriveAgentControlAttemptId(first, 1),
    );
    assert.notEqual(
      yield* deriveAgentControlAttemptId(first, 1),
      yield* deriveAgentControlAttemptId(first, 2),
    );
  }),
);

it.effect("source identity fingerprint excludes issue body", () =>
  Effect.gen(function* () {
    assert.equal(
      yield* deriveAgentControlSourceIdentityFingerprint(task("first secret body")),
      yield* deriveAgentControlSourceIdentityFingerprint(task("changed secret body")),
    );
  }),
);
