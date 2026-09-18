import { epicDependenciesSatisfied } from "./dependencyPlan.ts";
import type {
  AgentControlEpicBlocker,
  AgentControlEpicRuntimeView,
  AgentControlEpicSource,
  AgentControlEpicTaskSource,
} from "@t3tools/contracts";
import { epicDigest } from "./authority.ts";

const identity = (issue: AgentControlEpicTaskSource["issue"]) => ({
  repositoryNodeId: issue.repositoryNodeId,
  issueNodeId: issue.issueNodeId,
  number: issue.number,
  subIssueCount: issue.subIssueCount,
});

/** Text and closure changes cannot silently replace the accepted membership or edges. */
export const epicStructureDigest = (source: AgentControlEpicSource) =>
  epicDigest({
    repository: source.repository.repositoryNodeId,
    epic: identity(source.epic),
    ...(source.dependencies && source.dependencies.length > 0
      ? {
          dependencies: source.dependencies
            .map(identity)
            .toSorted((a, b) => a.issueNodeId.localeCompare(b.issueNodeId)),
        }
      : {}),
    tasks: source.tasks.map((task) => ({
      issue: identity(task.issue),
      position: task.position,
      // GitHub dependency response order does not define execution order.
      dependencies: task.dependencies
        .map(identity)
        .toSorted((a, b) => a.issueNodeId.localeCompare(b.issueNodeId)),
    })),
  });

export const selectEpicMember = (
  epic: AgentControlEpicRuntimeView,
  eligibleIssueIds?: ReadonlySet<string>,
  verifiedExternalIssueIds?: ReadonlySet<string>,
) => {
  const satisfied = new Set(
    epic.members
      .filter((member) => member.status === "accepted" || member.status === "external-closed")
      .map((member) => member.issueNodeId),
  );
  for (const dependency of epic.externalPrerequisites ?? []) satisfied.add(dependency.issueNodeId);
  return (
    [...epic.source.tasks]
      .sort((a, b) => a.position - b.position || a.issue.number - b.issue.number)
      .find(
        (task) =>
          (eligibleIssueIds === undefined || eligibleIssueIds.has(task.issue.issueNodeId)) &&
          epic.members.some(
            (member) =>
              member.issueNodeId === task.issue.issueNodeId && member.status === "pending",
          ) &&
          (epic.dependencyPlan
            ? epicDependenciesSatisfied(epic, task.issue.issueNodeId, verifiedExternalIssueIds)
            : task.dependencies.every(
                (dependency) =>
                  satisfied.has(dependency.issueNodeId) ||
                  (dependency.state === "closed" &&
                    !epic.members.some((member) => member.issueNodeId === dependency.issueNodeId)),
              )),
      ) ?? null
  );
};

export const epicSourceChanges = (
  epic: AgentControlEpicRuntimeView,
  current: AgentControlEpicSource,
): ReadonlyArray<AgentControlEpicBlocker> => {
  if (epicStructureDigest(epic.source) !== epicStructureDigest(current))
    return [
      {
        code: "scope-changed",
        issueNumber: null,
        message:
          "Epic membership or dependencies changed. Restore the accepted scope or stop this run and start a new one.",
      },
    ];
  const blockers: Array<AgentControlEpicBlocker> = [];
  if (epic.dependencyPlan) {
    const frozenIssues = [epic.source.epic, ...epic.source.tasks.map((task) => task.issue)];
    const currentIssues = [current.epic, ...current.tasks.map((task) => task.issue)];
    for (const frozen of frozenIssues) {
      const observed = currentIssues.find((issue) => issue.issueNodeId === frozen.issueNodeId);
      if (
        observed &&
        (observed.title !== frozen.title ||
          (frozen.contentFingerprint !== undefined &&
            observed.contentFingerprint !== frozen.contentFingerprint))
      )
        blockers.push({
          code: "scope-changed",
          issueNumber: frozen.number,
          message:
            "Issue content changed after the dependency plan was approved. Review independence again in a new Epic run.",
        });
    }
  }
  for (const dependency of current.dependencies ?? []) {
    if (dependency.repositoryNodeId !== current.repository.repositoryNodeId)
      blockers.push({
        code: "cross-repository",
        issueNumber: current.epic.number,
        message: "The Epic depends on an issue in another repository.",
      });
    else if (dependency.state === "open" && !epic.projectDependencyPlan) {
      const wasClosed = epic.source.dependencies?.some(
        (accepted) =>
          accepted.issueNodeId === dependency.issueNodeId && accepted.state === "closed",
      );
      blockers.push({
        code: wasClosed ? "prerequisite-reopened" : "missing-prerequisite",
        issueNumber: dependency.number,
        message: wasClosed
          ? `Epic prerequisite #${dependency.number} was reopened.`
          : `The Epic waits for open prerequisite #${dependency.number}.`,
      });
    }
  }
  if (current.epic.state === "closed")
    blockers.push({
      code: "closed-epic",
      issueNumber: current.epic.number,
      message: "The Epic was closed during execution.",
    });
  for (const member of epic.members) {
    const task = current.tasks.find((task) => task.issue.issueNodeId === member.issueNodeId);
    if (!task)
      blockers.push({
        code: "missing-issue",
        issueNumber: member.issueNumber,
        message: "An accepted Epic member is missing from GitHub.",
      });
    else if (
      (member.status === "pending" || member.status === "running") &&
      task.issue.state === "closed"
    )
      blockers.push({
        code: "closed-during-run",
        issueNumber: member.issueNumber,
        message:
          "This issue was closed before its result was integrated. Reopen it and resume, or stop this Epic.",
      });
    else if (member.status === "external-closed" && task.issue.state !== "closed")
      blockers.push({
        code: "prerequisite-reopened",
        issueNumber: member.issueNumber,
        message: "A prerequisite accepted as externally closed was reopened.",
      });
    for (const dependency of task?.dependencies ?? []) {
      const frozen = epic.source.tasks
        .find((item) => item.issue.issueNodeId === member.issueNodeId)
        ?.dependencies.find((item) => item.issueNodeId === dependency.issueNodeId);
      if (
        (frozen?.state === "closed" ||
          epic.externalPrerequisites?.some(
            (item) => item.issueNodeId === dependency.issueNodeId,
          )) &&
        dependency.state === "open" &&
        !epic.members.some((item) => item.issueNodeId === dependency.issueNodeId)
      )
        blockers.push({
          code: "prerequisite-reopened",
          issueNumber: dependency.number,
          message: "An externally closed prerequisite was reopened.",
        });
    }
  }
  return blockers;
};
