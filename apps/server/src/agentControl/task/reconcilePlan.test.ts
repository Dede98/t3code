import {
  AgentControlTaskId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";

import { buildReconcilePlan } from "./reconcilePlan.ts";

const projectId = ProjectId.make("task-reconcile-plan");
const repositoryNodeId = "repo-node";
const now = "2026-07-23T10:00:00.000Z";
const issue = (
  number: number,
  overrides: Partial<AgentControlGithubIssueSnapshot> = {},
): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId,
  issueNodeId: `issue-${number}`,
  number,
  url: `https://github.test/o/r/issues/${number}`,
  state: "open",
  title: `issue ${number}`,
  body: null,
  contentTrust: "untrusted-external",
  updatedAt: now,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
  ...overrides,
});
const task = (number: number): AgentControlTaskState => {
  const sourceIssue = issue(number);
  return {
    schemaVersion: 1,
    taskId: AgentControlTaskId.make(`task-${number}`),
    source: {
      projectId,
      repositoryNodeId,
      issueNodeId: sourceIssue.issueNodeId,
      issueNumber: number,
      issueUrl: sourceIssue.url,
    },
    status: "candidate",
    sourceGate: "eligible",
    stage: "intake",
    sourceUpdatedAt: now,
    githubIntakeSequence: 1,
    sourceSnapshot: {
      repositoryNodeId,
      issueNodeId: sourceIssue.issueNodeId,
      number,
      url: sourceIssue.url,
      state: "open",
      title: sourceIssue.title,
      body: null,
      contentTrust: "untrusted-external",
      updatedAt: now,
      timelineComplete: true,
      ready: true,
      paused: false,
      eligible: true,
      eligibilityReason: "eligible",
    },
    createdAt: now,
    updatedAt: now,
    revision: 1,
    sequence: 1,
  };
};
const precondition = (expectedIssueCount: number) => ({
  schemaVersion: 1 as const,
  projectId,
  githubIntakeSequence: 2,
  githubProjectionRevision: 2,
  githubConfigRevision: 2,
  repositoryNodeId,
  pollStatus: "success" as const,
  expectedIssueCount,
});

it("classifies conflicts, missing tasks, exact matches, new candidates, and ineligible issues once", () => {
  const issues = [
    issue(1, { issueNodeId: "replacement-1" }),
    issue(3),
    issue(4),
    issue(5, {
      ready: false,
      eligible: false,
      eligibilityReason: "ready-inactive",
    }),
  ];
  const plan = buildReconcilePlan({ sourcePrecondition: precondition(issues.length), issues }, [
    task(1),
    task(2),
    task(3),
  ]);

  assert.equal(plan.classifiable, true);
  assert.deepStrictEqual(
    plan.issueClassifications.map((classification) => classification.kind),
    ["local-conflict", "exact-match", "new-eligible", "ineligible"],
  );
  assert.deepStrictEqual(
    plan.taskClassifications.map((classification) => classification.kind),
    ["local-conflict", "source-missing", "exact-match"],
  );
  assert.deepStrictEqual(
    plan.operations.map((operation) => operation.type),
    ["mark-identity-invalid", "mark-source-missing", "refresh", "create", "unchanged"],
  );
  assert.equal(plan.issueClassifications.length, issues.length);
  assert.equal(plan.taskClassifications.length, 3);
});

it("fails closed without overwriting duplicate snapshot identity classifications", () => {
  const duplicate = issue(10);
  const plan = buildReconcilePlan(
    {
      sourcePrecondition: precondition(2),
      issues: [duplicate, { ...duplicate, title: "different duplicate" }],
    },
    [],
  );
  assert.equal(plan.classifiable, false);
  assert.deepStrictEqual(
    plan.issueClassifications.map((classification) => classification.kind),
    ["unclassifiable", "unclassifiable"],
  );
});
