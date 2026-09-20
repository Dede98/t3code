import {
  ProjectId,
  type AgentControlEpicProjectDependencyPlan,
  type AgentControlEpicQueueEntry,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicSource,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { epicDigest } from "./authority.ts";
import { epicDependenciesSatisfied } from "./dependencyPlan.ts";
import { selectEpicMember } from "./model.ts";
import {
  projectTaskPrerequisites,
  validateProjectDependencyPlan,
} from "./projectDependencyPlan.ts";
import { createEpicRun } from "./runState.ts";

const issue = (number: number) => ({
  repositoryNodeId: "repo",
  nameWithOwner: "owner/repo",
  issueNodeId: String(number),
  number,
  title: String(number),
  url: `https://example.test/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});
const entry = (number: number, tasks: number[]): AgentControlEpicQueueEntry => {
  const source: AgentControlEpicSource = {
    format: "github-native-sub-issues-v1",
    repository: { repositoryNodeId: "repo", nameWithOwner: "owner/repo" },
    epic: issue(number),
    tasks: tasks.map((number, position) => ({ issue: issue(number), position, dependencies: [] })),
    blockers: [],
    fingerprint: `scope-${number}`,
    inspectedAt: "2026-09-17T12:00:00.000Z",
  };
  return {
    entryId: String(number),
    source,
    approvedAt: source.inspectedAt,
    epicRunId: null,
    status: "pending",
    blockers: [],
    parallelism: 2,
    dependencyPlan: {
      version: 1,
      sourceFingerprint: source.fingerprint,
      rationale: "Reviewed separate files.",
      tasks: tasks.map((number) => ({ issueNodeId: String(number), dependsOn: [] })),
    },
  };
};
const entries = [entry(100, [1, 2]), entry(200, [3, 4])];
const plan: AgentControlEpicProjectDependencyPlan = {
  version: 1,
  rationale: "Separate documents; task 3 consumes the reviewed merge of task 1.",
  epics: entries.map((entry) => ({
    issueNodeId: entry.source.epic.issueNodeId,
    sourceFingerprint: entry.source.fingerprint,
  })),
  tasks: [1, 2, 3, 4].map((number) => ({
    issueNodeId: String(number),
    dependsOn: number === 3 ? ["1"] : [],
  })),
};
const run = Effect.fn("makeProjectPlanRun")(function* (entry: AgentControlEpicQueueEntry) {
  return {
    ...(yield* createEpicRun({
      projectId: ProjectId.make("project"),
      commandId: entry.entryId,
      source: entry.source,
      checks: [],
      parallelism: entry.parallelism ?? 1,
      ...(entry.dependencyPlan ? { dependencyPlan: entry.dependencyPlan } : {}),
    })),
    projectDependencyPlan: plan,
    projectDependencyPlanDigest: epicDigest(plan),
  } satisfies AgentControlEpicRuntimeView;
});

describe("reviewed project dependency plan", () => {
  it.effect("retains a merged serial Epic when approving subsequent parallel work", () =>
    Effect.gen(function* () {
      const { dependencyPlan: _previousPlan, ...serial } = entries[0]!;
      const merged = { ...serial, status: "merged" as const, epicRunId: "merged-serial-run" };
      yield* validateProjectDependencyPlan([merged, entries[1]!], plan, 2);
      for (const status of ["pending", "active", "stopped"] as const) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              validateProjectDependencyPlan([{ ...merged, status }, entries[1]!], plan, 2),
            ),
          ),
        );
      }
      const native = {
        ...merged,
        source: {
          ...merged.source,
          tasks: merged.source.tasks.map((task) =>
            task.issue.number === 2 ? { ...task, dependencies: [issue(1)] } : task,
          ),
        },
      };
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(validateProjectDependencyPlan([native, entries[1]!], plan, 2)),
        ),
      );
      yield* validateProjectDependencyPlan(
        [native, entries[1]!],
        {
          ...plan,
          tasks: plan.tasks.map((task) =>
            task.issueNodeId === "2" ? { ...task, dependsOn: ["1"] } : task,
          ),
        },
        2,
      );
    }),
  );
  it.effect(
    "preserves serial opt-in and requires complete reviewed scopes for parallel Epics",
    () =>
      Effect.gen(function* () {
        yield* validateProjectDependencyPlan(entries, undefined, 1);
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(entries, undefined, 2))),
        );
        yield* validateProjectDependencyPlan(entries, plan, 2);
        for (const invalid of [
          { ...plan, epics: plan.epics.slice(1) },
          { ...plan, epics: plan.epics.map((epic) => ({ ...epic, sourceFingerprint: "changed" })) },
          { ...plan, tasks: plan.tasks.slice(1) },
          { ...plan, tasks: [...plan.tasks, plan.tasks[0]!] },
          {
            ...plan,
            tasks: plan.tasks.map((task) =>
              task.issueNodeId === "3" ? { ...task, dependsOn: ["99"] } : task,
            ),
          },
        ])
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(entries, invalid, 2))),
          );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              validateProjectDependencyPlan([entries[0]!, entry(200, [1, 3])], plan, 2),
            ),
          ),
        );
      }),
  );
  it.effect("expands a native task dependency on another Epic to all of its reviewed tasks", () =>
    Effect.gen(function* () {
      const native = entries.map((entry) =>
        entry.source.epic.number === 200
          ? {
              ...entry,
              source: {
                ...entry.source,
                tasks: entry.source.tasks.map((task) =>
                  task.issue.number === 3 ? { ...task, dependencies: [issue(100)] } : task,
                ),
              },
              dependencyPlan: {
                ...entry.dependencyPlan!,
                tasks: entry.dependencyPlan!.tasks.map((task) =>
                  task.issueNodeId === "3" ? { ...task, dependsOn: ["100"] } : task,
                ),
              },
            }
          : entry,
      );
      const expanded = {
        ...plan,
        tasks: plan.tasks.map((task) =>
          task.issueNodeId === "3" ? { ...task, dependsOn: ["1", "2"] } : task,
        ),
      };
      yield* validateProjectDependencyPlan(native, expanded, 2);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(native, plan, 2))),
      );
    }),
  );
  it.effect(
    "never drops native or already-reviewed dependencies, including Epic-level prerequisites",
    () =>
      Effect.gen(function* () {
        const native = entries.map((entry) =>
          entry.source.epic.number === 200
            ? {
                ...entry,
                source: {
                  ...entry.source,
                  tasks: entry.source.tasks.map((task) =>
                    task.issue.number === 4 ? { ...task, dependencies: [issue(2)] } : task,
                  ),
                },
              }
            : entry,
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(native, plan, 2))),
        );
        const epicEdge = entries.map((entry) =>
          entry.source.epic.number === 200
            ? { ...entry, source: { ...entry.source, dependencies: [issue(100)] } }
            : entry,
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(epicEdge, plan, 2))),
        );
        yield* validateProjectDependencyPlan(
          epicEdge,
          {
            ...plan,
            tasks: plan.tasks.map((task) =>
              Number(task.issueNodeId) > 2 ? { ...task, dependsOn: ["1", "2"] } : task,
            ),
          },
          2,
        );
        const reviewed = entries.map((entry) =>
          entry.source.epic.number === 100
            ? {
                ...entry,
                dependencyPlan: {
                  ...entry.dependencyPlan!,
                  tasks: [
                    { issueNodeId: "1", dependsOn: [] },
                    { issueNodeId: "2", dependsOn: ["1"] },
                  ],
                },
              }
            : entry,
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(reviewed, plan, 2))),
        );
      }),
  );
  it.effect(
    "rejects task cycles and acyclic task graphs that deadlock at human review boundaries",
    () =>
      Effect.gen(function* () {
        // 1 and 4 are roots; 2 waits for 4 and 3 waits for 1. Neither Epic can finish its PR.
        const reviewCycle = {
          ...plan,
          tasks: plan.tasks.map((task) =>
            task.issueNodeId === "2" ? { ...task, dependsOn: ["4"] } : task,
          ),
        };
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(validateProjectDependencyPlan(entries, reviewCycle, 2)),
          ),
        );
        const taskCycle = {
          ...plan,
          tasks: plan.tasks.map((task) =>
            task.issueNodeId === "1" ? { ...task, dependsOn: ["3"] } : task,
          ),
        };
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateProjectDependencyPlan(entries, taskCycle, 2))),
        );
      }),
  );
  it.effect(
    "keeps independent tasks runnable while a dependency waits for its reviewed merge",
    () =>
      Effect.gen(function* () {
        const first = yield* run(entries[0]!);
        const second = yield* run(entries[1]!);
        assert.lengthOf(projectTaskPrerequisites(second, "3", [first, second]).blockers, 1);
        assert.lengthOf(projectTaskPrerequisites(second, "4", [first, second]).blockers, 0);
        assert.equal(selectEpicMember(second)?.issue.number, 4);
        assert.isFalse(epicDependenciesSatisfied(second, "3"));
        assert.isTrue(epicDependenciesSatisfied(second, "3", new Set(["1"])));
        assert.isFalse(
          epicDependenciesSatisfied(
            { ...second, projectDependencyPlanDigest: "corrupt" },
            "3",
            new Set(["1"]),
          ),
        );
      }),
  );
  it.effect(
    "requires accepted current-head evidence and a matching human merge, never closure alone",
    () =>
      Effect.gen(function* () {
        const first = yield* run(entries[0]!);
        const second = yield* run(entries[1]!);
        const head = "a".repeat(40);
        const merge = "b".repeat(40);
        const completed: AgentControlEpicRuntimeView = {
          ...first,
          status: "succeeded",
          acceptedCommitSha: head,
          members: first.members.map((member) => ({
            ...member,
            status: "accepted",
            accepted: {
              commitSha: head,
              treeSha: "tree",
              codeDigest: "code",
              evidenceId: "capture",
            },
          })),
          finalVerification: {
            status: "passed",
            commitSha: head,
            evidenceId: "verify",
            detail: "Passed",
            checks: [],
          },
          handoff: {
            intentId: "handoff",
            status: "published",
            repository: {
              repositoryNodeId: "repo",
              nameWithOwner: "owner/repo",
            },
            targetBranch: "main",
            baseCommitSha: "c".repeat(40),
            commitSha: head,
            branchName: "epic-a",
            verificationEvidenceId: "verify",
            requestedAt: first.createdAt,
            updatedAt: first.updatedAt,
            pullRequest: {
              number: 9,
              url: "https://github.com/owner/repo/pull/9",
              state: "merged",
              isDraft: false,
              headSha: head,
              baseBranch: "main",
              mergeCommitSha: merge,
            },
            error: null,
          },
        };
        assert.equal(
          projectTaskPrerequisites(second, "3", [completed, second]).prerequisites[0]
            ?.mergeCommitSha,
          merge,
        );
        for (const changed of [
          { ...completed, status: "running" as const },
          { ...completed, acceptedCommitSha: "different" },
          { ...completed, source: { ...completed.source, fingerprint: "changed" } },
          {
            ...completed,
            handoff: {
              ...completed.handoff!,
              pullRequest: { ...completed.handoff!.pullRequest!, state: "closed" as const },
            },
          },
          {
            ...completed,
            handoff: {
              ...completed.handoff!,
              pullRequest: { ...completed.handoff!.pullRequest!, mergeCommitSha: null },
            },
          },
        ])
          assert.lengthOf(projectTaskPrerequisites(second, "3", [changed, second]).blockers, 1);
        assert.lengthOf(
          projectTaskPrerequisites(second, "3", [
            completed,
            { ...completed, epicRunId: "duplicate" },
            second,
          ]).blockers,
          1,
        );
      }),
  );
});
