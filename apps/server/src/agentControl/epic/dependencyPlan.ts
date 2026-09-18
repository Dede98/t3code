import {
  AgentControlEpicDependencyPlan,
  AgentControlEpicParallelism,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicSource,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { epicDigest, epicError } from "./authority.ts";

const decodeParallelism = Schema.decodeUnknownEffect(AgentControlEpicParallelism);
const decodePlan = Schema.decodeUnknownEffect(AgentControlEpicDependencyPlan);

/** Approval attests completeness and semantic independence, not merely missing GitHub edges. */
export const validateEpicDependencyPlan = Effect.fn("validateEpicDependencyPlan")(function* (
  source: AgentControlEpicSource,
  plan: AgentControlEpicDependencyPlan | undefined,
  parallelism = 1,
  externalIssueIds: ReadonlySet<string> = new Set(),
) {
  yield* decodeParallelism(parallelism);
  if (plan === undefined) {
    if (parallelism > 1)
      return yield* epicError(
        "dependencies-unknown",
        "Parallel execution requires a reviewed, complete dependency plan and independence rationale.",
      );
    return;
  }
  yield* decodePlan(plan);
  const reject = (message: string) => epicError("invalid-dependency-plan", message);
  if (plan.sourceFingerprint !== source.fingerprint)
    return yield* reject("The dependency plan belongs to another Epic preview.");
  const nodes = new Map(plan.tasks.map((task) => [task.issueNodeId, task]));
  if (nodes.size !== plan.tasks.length || nodes.size !== source.tasks.length)
    return yield* reject("The plan must describe every Epic task exactly once.");
  for (const task of source.tasks) {
    const node = nodes.get(task.issue.issueNodeId);
    if (!node) return yield* reject(`Missing dependency description for #${task.issue.number}.`);
    if (task.issue.state === "closed")
      return yield* reject(
        `Closed issue #${task.issue.number} has no integrated verification evidence. Reopen it before approving this plan.`,
      );
    if (new Set(node.dependsOn).size !== node.dependsOn.length)
      return yield* reject(`Duplicate dependency for #${task.issue.number}.`);
    for (const dependency of node.dependsOn) {
      if (!nodes.has(dependency) && !externalIssueIds.has(dependency))
        return yield* reject(
          `Unknown dependency ${dependency}. Dependencies must belong to the reviewed Epic or project plan.`,
        );
      if (dependency === node.issueNodeId)
        return yield* reject(`Task #${task.issue.number} depends on itself.`);
    }
    if (task.dependencies.some((dependency) => !node.dependsOn.includes(dependency.issueNodeId)))
      return yield* reject(`The plan omits a native dependency of #${task.issue.number}.`);
  }
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const acyclic = (id: string): boolean => {
    if (!nodes.has(id)) return true;
    if (visiting.has(id)) return false;
    if (visited.has(id)) return true;
    visiting.add(id);
    if (!nodes.get(id)!.dependsOn.every(acyclic)) return false;
    visiting.delete(id);
    visited.add(id);
    return true;
  };
  if (![...nodes.keys()].every(acyclic))
    return yield* reject("The dependency plan contains a cycle.");
});

export const epicDependenciesSatisfied = (
  epic: AgentControlEpicRuntimeView,
  issueNodeId: string,
  verifiedExternalIssueIds: ReadonlySet<string> = new Set(),
) => {
  const plan = epic.dependencyPlan;
  if (!plan || epic.dependencyPlanDigest !== epicDigest(plan)) return false;
  const task = plan.tasks.find((task) => task.issueNodeId === issueNodeId);
  if (!task) return false;
  const projectPlan = epic.projectDependencyPlan;
  if (projectPlan && epic.projectDependencyPlanDigest !== epicDigest(projectPlan)) return false;
  const projectTask = projectPlan?.tasks.find((node) => node.issueNodeId === issueNodeId);
  if (projectPlan && !projectTask) return false;
  const dependencies = projectTask?.dependsOn ?? task.dependsOn;
  const local = dependencies.filter((id) =>
    epic.members.some((member) => member.issueNodeId === id),
  );
  if (
    dependencies.some(
      (id) => !local.includes(id) && (!projectPlan || !verifiedExternalIssueIds.has(id)),
    )
  )
    return false;
  if (local.length === 0) return true;
  return (
    epic.integrationVerification?.status === "passed" &&
    epic.integrationVerification.commitSha === epic.acceptedCommitSha &&
    local.every((dependency) =>
      epic.members.some(
        (member) =>
          member.issueNodeId === dependency &&
          member.status === "accepted" &&
          member.accepted !== null,
      ),
    )
  );
};
