import {
  AgentControlEpicActiveLimit,
  AgentControlEpicProjectDependencyPlan,
  type AgentControlEpicBlocker,
  type AgentControlEpicQueueEntry,
  type AgentControlEpicRuntimeView,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { epicDigest, epicError } from "./authority.ts";
import { validateEpicDependencyPlan } from "./dependencyPlan.ts";

const decodeLimit = Schema.decodeUnknownEffect(AgentControlEpicActiveLimit);
const decodePlan = Schema.decodeUnknownEffect(AgentControlEpicProjectDependencyPlan);

const acyclic = (edges: ReadonlyMap<string, readonly string[]>) => {
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const visit = (id: string): boolean => {
    if (visiting.has(id)) return false;
    if (visited.has(id)) return true;
    visiting.add(id);
    if (!(edges.get(id) ?? []).every(visit)) return false;
    visiting.delete(id);
    visited.add(id);
    return true;
  };
  return [...edges.keys()].every(visit);
};

/** Human approval covers the complete graph; missing GitHub edges never imply independence. */
export const validateProjectDependencyPlan = Effect.fn("validateProjectDependencyPlan")(function* (
  entries: readonly AgentControlEpicQueueEntry[],
  plan: AgentControlEpicProjectDependencyPlan | undefined,
  maxActiveEpics = 1,
) {
  yield* decodeLimit(maxActiveEpics);
  if (!plan) {
    if (maxActiveEpics > 1)
      return yield* epicError(
        "dependencies-unknown",
        "Parallel Epics require a reviewed project dependency plan.",
      );
    return;
  }
  yield* decodePlan(plan);
  const reject = (message: string) => epicError("invalid-dependency-plan", message);
  const epics = new Map(entries.map((entry) => [entry.source.epic.issueNodeId, entry]));
  if (
    epics.size !== entries.length ||
    plan.epics.length !== entries.length ||
    new Set(plan.epics.map((epic) => epic.issueNodeId)).size !== entries.length
  )
    return yield* reject("The project plan must describe every approved Epic exactly once.");
  for (const approved of plan.epics) {
    const entry = epics.get(approved.issueNodeId);
    if (!entry || entry.source.fingerprint !== approved.sourceFingerprint)
      return yield* reject("The project plan belongs to another approved Epic scope.");
  }
  const owners = new Map<string, AgentControlEpicQueueEntry>();
  for (const entry of entries)
    for (const task of entry.source.tasks) {
      if (owners.has(task.issue.issueNodeId))
        return yield* reject(
          `Task #${task.issue.number} belongs to multiple Epics. Assign it to one Epic before approval.`,
        );
      owners.set(task.issue.issueNodeId, entry);
    }
  const nodes = new Map(plan.tasks.map((task) => [task.issueNodeId, task.dependsOn]));
  if (
    nodes.size !== plan.tasks.length ||
    nodes.size !== owners.size ||
    [...owners.keys()].some((id) => !nodes.has(id))
  )
    return yield* reject("The project plan must describe every task exactly once.");
  for (const [id, dependencies] of nodes) {
    if (
      new Set(dependencies).size !== dependencies.length ||
      dependencies.includes(id) ||
      dependencies.some((dependency) => !nodes.has(dependency))
    )
      return yield* reject(
        "Project dependencies must be unique references to other approved tasks.",
      );
  }
  const allIds = new Set([...nodes.keys(), ...epics.keys()]);
  const expand = (id: string) =>
    epics.get(id)?.source.tasks.map((task) => task.issue.issueNodeId) ?? [id];
  for (const entry of entries) {
    if (!entry.dependencyPlan)
      return yield* reject(
        "Each Epic needs a reviewed task plan before project parallelism is approved.",
      );
    yield* validateEpicDependencyPlan(
      entry.source,
      entry.dependencyPlan,
      entry.parallelism,
      allIds,
    );
    for (const task of entry.source.tasks) {
      const dependencies = nodes.get(task.issue.issueNodeId)!;
      const local = entry.dependencyPlan.tasks.find(
        (node) => node.issueNodeId === task.issue.issueNodeId,
      )!;
      if (
        local.dependsOn.flatMap(expand).some((id) => !dependencies.includes(id)) ||
        task.dependencies
          .flatMap((dependency) => expand(dependency.issueNodeId))
          .some((id) => !dependencies.includes(id))
      )
        return yield* reject(
          `The project plan omits a reviewed or native dependency of #${task.issue.number}.`,
        );
      // An Epic-level dependency gates every task behind that Epic's reviewed merge.
      for (const dependency of entry.source.dependencies ?? []) {
        const prerequisite = epics.get(dependency.issueNodeId);
        const required =
          prerequisite?.source.tasks.map((item) => item.issue.issueNodeId) ??
          (owners.has(dependency.issueNodeId) ? [dependency.issueNodeId] : []);
        if (required.length === 0 || required.some((id) => !dependencies.includes(id)))
          return yield* reject(`The project plan omits Epic prerequisite #${dependency.number}.`);
      }
    }
  }
  if (!acyclic(nodes)) return yield* reject("The project task dependency plan contains a cycle.");
  // Even an acyclic task graph deadlocks when each Epic requires the other's complete PR merge.
  const reviewEdges = new Map<string, string[]>();
  for (const [id, dependencies] of nodes) {
    const owner = owners.get(id)!.source.epic.issueNodeId;
    const previous = reviewEdges.get(owner) ?? [];
    reviewEdges.set(owner, [
      ...new Set([
        ...previous,
        ...dependencies
          .map((dependency) => owners.get(dependency)!.source.epic.issueNodeId)
          .filter((dependency) => dependency !== owner),
      ]),
    ]);
  }
  if (!acyclic(reviewEdges))
    return yield* reject(
      "The project plan creates a cycle across human review and merge boundaries.",
    );
});

/** Resolves only persisted approvals and results. Git ancestry is proven separately before start. */
export const projectTaskPrerequisites = (
  epic: AgentControlEpicRuntimeView,
  issueNodeId: string,
  runs: readonly AgentControlEpicRuntimeView[],
) => {
  const blockers: AgentControlEpicBlocker[] = [];
  const prerequisites: {
    issueNodeId: string;
    run: AgentControlEpicRuntimeView;
    mergeCommitSha: string;
  }[] = [];
  const plan = epic.projectDependencyPlan;
  if (!plan) return { blockers, prerequisites };
  const block = (message: string) =>
    blockers.push({ code: "project-dependency-wait", issueNumber: null, message });
  if (epic.projectDependencyPlanDigest !== epicDigest(plan)) {
    block("The frozen project dependency approval is invalid.");
    return { blockers, prerequisites };
  }
  const node = plan.tasks.find((task) => task.issueNodeId === issueNodeId);
  if (!node) {
    block("This task is absent from the frozen project dependency plan.");
    return { blockers, prerequisites };
  }
  for (const dependency of node.dependsOn) {
    if (epic.members.some((member) => member.issueNodeId === dependency)) continue;
    const matches = runs.filter(
      (run) =>
        run.projectId === epic.projectId &&
        run.source.repository.repositoryNodeId === epic.source.repository.repositoryNodeId &&
        plan.epics.some(
          (approved) =>
            approved.issueNodeId === run.source.epic.issueNodeId &&
            approved.sourceFingerprint === run.source.fingerprint,
        ) &&
        run.members.some((member) => member.issueNodeId === dependency),
    );
    const run = matches.length === 1 ? matches[0] : undefined;
    const member = run?.members.find((item) => item.issueNodeId === dependency);
    const pr = run?.handoff?.pullRequest;
    if (
      !run ||
      run.status !== "succeeded" ||
      member?.status !== "accepted" ||
      !member.accepted ||
      run.finalVerification?.status !== "passed" ||
      run.finalVerification.commitSha !== run.acceptedCommitSha ||
      run.handoff?.commitSha !== run.acceptedCommitSha ||
      pr?.state !== "merged" ||
      !pr.mergeCommitSha ||
      !/^[a-f\d]{40,64}$/i.test(pr.mergeCommitSha) ||
      pr.headSha !== run.handoff.commitSha ||
      pr.baseBranch !== run.handoff.targetBranch ||
      (epic.initialBase && pr.baseBranch !== epic.initialBase.targetBranch)
    ) {
      block(
        `Waiting for the reviewed merge of prerequisite ${member ? `#${member.issueNumber}` : dependency}. Verification or a closed issue alone does not satisfy it.`,
      );
      continue;
    }
    prerequisites.push({ issueNodeId: dependency, run, mergeCommitSha: pr.mergeCommitSha });
  }
  return { blockers, prerequisites };
};
