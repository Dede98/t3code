import {
  ProjectId,
  type AgentControlEpicDependencyPlan,
  type AgentControlEpicSource,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { epicDigest } from "./authority.ts";
import { epicDependenciesSatisfied, validateEpicDependencyPlan } from "./dependencyPlan.ts";
import { createEpicRun } from "./runState.ts";
import { epicSourceChanges, selectEpicMember } from "./model.ts";

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
const source: AgentControlEpicSource = {
  format: "github-native-sub-issues-v1",
  repository: { repositoryNodeId: "repo", nameWithOwner: "owner/repo" },
  epic: issue(4),
  tasks: [1, 2, 3].map((number) => ({
    issue: issue(number),
    position: number,
    dependencies: number === 3 ? [issue(1), issue(2)] : [],
  })),
  blockers: [],
  fingerprint: "preview",
  inspectedAt: "2026-09-17T12:00:00.000Z",
};
const plan: AgentControlEpicDependencyPlan = {
  version: 1,
  sourceFingerprint: source.fingerprint,
  rationale: "A and B edit separate Markdown documents; C combines their verified results.",
  tasks: source.tasks.map((task) => ({
    issueNodeId: task.issue.issueNodeId,
    dependsOn: task.dependencies.map((dependency) => dependency.issueNodeId),
  })),
};
const make = () =>
  createEpicRun({
    projectId: ProjectId.make("plan"),
    commandId: "approve",
    source,
    checks: [],
    parallelism: 2,
    dependencyPlan: plan,
  });

describe("reviewed Epic dependency plan", () => {
  it.effect(
    "invalidates planned semantic approval after prose changes while retaining legacy behavior",
    () =>
      Effect.gen(function* () {
        const state = yield* make();
        const frozen = {
          ...state,
          source: {
            ...source,
            tasks: source.tasks.map((task) => ({
              ...task,
              issue: { ...task.issue, contentFingerprint: "approved-content" },
            })),
          },
        };
        const changed = {
          ...frozen.source,
          tasks: frozen.source.tasks.map((task, index) =>
            index === 0
              ? { ...task, issue: { ...task.issue, contentFingerprint: "changed-body" } }
              : task,
          ),
        };
        assert.equal(epicSourceChanges(frozen, changed)[0]?.code, "scope-changed");
        assert.equal(
          epicSourceChanges(state, { ...source, epic: { ...source.epic, title: "new intent" } })[0]
            ?.code,
          "scope-changed",
        );
        const { dependencyPlan: _plan, dependencyPlanDigest: _digest, ...legacy } = frozen;
        assert.deepStrictEqual(epicSourceChanges(legacy, changed), []);
        assert.deepStrictEqual(epicSourceChanges(frozen, frozen.source), []);
      }),
  );
  it.effect(
    "distinguishes unknown dependencies from explicitly reviewed roots and preserves serial default",
    () =>
      Effect.gen(function* () {
        yield* validateEpicDependencyPlan(source, undefined, 1);
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateEpicDependencyPlan(source, undefined, 2))),
        );
        yield* validateEpicDependencyPlan(source, plan, 2);
        const state = yield* make();
        assert.isTrue(epicDependenciesSatisfied(state, "1"));
        assert.isFalse(epicDependenciesSatisfied(state, "3"));
        assert.equal(selectEpicMember(state)?.issue.number, 1);
      }),
  );
  for (const [name, tasks] of [
    ["unknown reference", [{ issueNodeId: "1", dependsOn: ["99"] }, ...plan.tasks.slice(1)]],
    ["self dependency", [{ issueNodeId: "1", dependsOn: ["1"] }, ...plan.tasks.slice(1)]],
    ["cycle", [{ issueNodeId: "1", dependsOn: ["3"] }, ...plan.tasks.slice(1)]],
    ["missing description", plan.tasks.slice(1)],
    ["duplicate task", [plan.tasks[0]!, plan.tasks[0]!, plan.tasks[2]!]],
    ["omitted native edge", [...plan.tasks.slice(0, 2), { issueNodeId: "3", dependsOn: [] }]],
  ] as const)
    it.effect(`rejects ${name}`, () =>
      Effect.gen(function* () {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(validateEpicDependencyPlan(source, { ...plan, tasks }, 2)),
          ),
        );
      }),
    );
  it.effect(
    "rejects foreign preview, closed predecessor, invalid version and excess capacity",
    () =>
      Effect.gen(function* () {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              validateEpicDependencyPlan(source, { ...plan, sourceFingerprint: "other" }, 2),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              validateEpicDependencyPlan(
                {
                  ...source,
                  tasks: source.tasks.map((task) => ({
                    ...task,
                    issue: { ...task.issue, state: "closed" },
                  })),
                },
                plan,
                2,
              ),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(validateEpicDependencyPlan(source, plan, 5))),
        );
      }),
  );
  it.effect("releases C only for integrated A and B and proof of the current head", () =>
    Effect.gen(function* () {
      const state = yield* make();
      const result = {
        commitSha: "head",
        treeSha: "tree",
        codeDigest: "digest",
        evidenceId: "proof",
      };
      const integrated = {
        ...state,
        acceptedCommitSha: "head",
        members: state.members.map((member) =>
          member.issueNumber < 3
            ? { ...member, status: "accepted" as const, accepted: result }
            : member,
        ),
        integrationVerification: {
          status: "passed" as const,
          commitSha: "head",
          evidenceId: "proof",
          detail: "Passed",
          checks: [],
        },
      };
      assert.isTrue(epicDependenciesSatisfied(integrated, "3"));
      assert.isFalse(
        epicDependenciesSatisfied({ ...integrated, acceptedCommitSha: "new-head" }, "3"),
      );
      assert.isFalse(
        epicDependenciesSatisfied(
          {
            ...integrated,
            members: integrated.members.map((member) =>
              member.issueNumber === 1 ? { ...member, status: "external-closed" } : member,
            ),
          },
          "3",
        ),
      );
      assert.isFalse(
        epicDependenciesSatisfied(
          { ...integrated, dependencyPlan: { ...plan, rationale: "changed" } },
          "3",
        ),
      );
      assert.equal(integrated.dependencyPlanDigest, epicDigest(plan));
    }),
  );
});
