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
          task.dependencies.every(
            (dependency) =>
              satisfied.has(dependency.issueNodeId) ||
              (dependency.state === "closed" &&
                !epic.members.some((member) => member.issueNodeId === dependency.issueNodeId)),
          ),
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
      (member.status === "pending" ||
        (member.status === "running" && member.childRunId === null)) &&
      task.issue.state === "closed"
    )
      blockers.push({
        code: "closed-during-run",
        issueNumber: member.issueNumber,
        message:
          "This pending issue was closed after the Epic scope was accepted. It has no T3 verification evidence.",
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
