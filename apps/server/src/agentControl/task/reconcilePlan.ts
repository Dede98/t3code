import type {
  AgentControlGithubIssueSnapshot,
  AgentControlTaskSourceGate,
  AgentControlTaskSourcePrecondition,
  AgentControlTaskSourceSnapshot,
  AgentControlTaskState,
} from "@t3tools/contracts";

import { sameTaskSourceSnapshot } from "./decider.ts";
import {
  canonicalAgentControlTaskSourceTimestamp,
  compareAgentControlTaskSourceTimestamps,
} from "./sourceTimestamp.ts";

const identityKey = (repositoryNodeId: string, issueNodeId: string) =>
  `${repositoryNodeId}\u0000${issueNodeId}`;
const numberKey = (repositoryNodeId: string, issueNumber: number) =>
  `${repositoryNodeId}\u0000${issueNumber}`;

const sourceGate = (issue: AgentControlGithubIssueSnapshot): AgentControlTaskSourceGate => {
  if (!issue.timelineComplete || issue.eligibilityReason === "timeline-invalid") {
    return "timeline-invalid";
  }
  if (issue.state === "closed" || issue.eligibilityReason === "closed") return "closed";
  if (issue.paused || issue.eligibilityReason === "paused") return "paused";
  return issue.eligible && issue.ready ? "eligible" : "not-ready";
};

const sourceSnapshot = (
  issue: AgentControlGithubIssueSnapshot,
  canonicalUpdatedAt: string,
): AgentControlTaskSourceSnapshot => ({
  repositoryNodeId: issue.repositoryNodeId,
  issueNodeId: issue.issueNodeId,
  number: issue.number,
  url: issue.url,
  state: issue.state,
  title: issue.title,
  body: issue.body,
  contentTrust: "untrusted-external",
  updatedAt: canonicalUpdatedAt,
  timelineComplete: issue.timelineComplete,
  ready: issue.ready,
  paused: issue.paused,
  eligible: issue.eligible,
  eligibilityReason: issue.eligibilityReason,
});

const addToMultiMap = <K, V>(map: Map<K, Array<V>>, key: K, value: V) => {
  const values = map.get(key);
  if (values === undefined) map.set(key, [value]);
  else values.push(value);
};

export type AgentControlTaskIssueClassification =
  | {
      readonly kind: "exact-match";
      readonly issue: AgentControlGithubIssueSnapshot;
      readonly task: AgentControlTaskState;
    }
  | {
      readonly kind: "new-eligible";
      readonly issue: AgentControlGithubIssueSnapshot;
    }
  | {
      readonly kind: "ineligible";
      readonly issue: AgentControlGithubIssueSnapshot;
    }
  | {
      readonly kind: "local-conflict";
      readonly issue: AgentControlGithubIssueSnapshot;
      readonly task: AgentControlTaskState;
    }
  | {
      readonly kind: "unclassifiable";
      readonly issue: AgentControlGithubIssueSnapshot;
    };

export type AgentControlTaskExistingClassification =
  | {
      readonly kind: "exact-match";
      readonly task: AgentControlTaskState;
      readonly issue: AgentControlGithubIssueSnapshot;
    }
  | {
      readonly kind: "local-conflict";
      readonly task: AgentControlTaskState;
    }
  | {
      readonly kind: "source-missing";
      readonly task: AgentControlTaskState;
    }
  | {
      readonly kind: "unchanged";
      readonly task: AgentControlTaskState;
    }
  | {
      readonly kind: "unclassifiable";
      readonly task: AgentControlTaskState;
    };

export type AgentControlTaskReconcileOperation =
  | {
      readonly type: "create";
      readonly issue: AgentControlGithubIssueSnapshot;
      readonly gate: "eligible";
      readonly snapshot: AgentControlTaskSourceSnapshot;
      readonly sourceUpdatedAt: string;
    }
  | {
      readonly type: "refresh";
      readonly task: AgentControlTaskState;
      readonly issue: AgentControlGithubIssueSnapshot;
      readonly gate: AgentControlTaskSourceGate;
      readonly snapshot: AgentControlTaskSourceSnapshot;
      readonly sourceUpdatedAt: string;
    }
  | {
      readonly type: "recover-source-missing";
      readonly task: AgentControlTaskState;
      readonly issue: AgentControlGithubIssueSnapshot;
      readonly snapshot: AgentControlTaskSourceSnapshot;
      readonly sourceUpdatedAt: string;
    }
  | {
      readonly type: "mark-identity-invalid";
      readonly task: AgentControlTaskState;
    }
  | {
      readonly type: "mark-source-missing";
      readonly task: AgentControlTaskState;
    }
  | {
      readonly type: "unchanged";
    };

export interface AgentControlTaskReconcilePlan {
  readonly classifiable: boolean;
  readonly issueClassifications: ReadonlyArray<AgentControlTaskIssueClassification>;
  readonly taskClassifications: ReadonlyArray<AgentControlTaskExistingClassification>;
  readonly operations: ReadonlyArray<AgentControlTaskReconcileOperation>;
}

interface Assignment {
  readonly issueIndex: number;
  readonly kind: "exact-match" | "local-conflict";
}

export const buildReconcilePlan = (
  snapshot: {
    readonly sourcePrecondition: AgentControlTaskSourcePrecondition;
    readonly issues: ReadonlyArray<AgentControlGithubIssueSnapshot>;
  },
  existingTasks: ReadonlyArray<AgentControlTaskState>,
): AgentControlTaskReconcilePlan => {
  const issueClassifications: Array<AgentControlTaskIssueClassification | null> = Array.from(
    { length: snapshot.issues.length },
    () => null,
  );
  const taskClassifications: Array<AgentControlTaskExistingClassification | null> = Array.from(
    { length: existingTasks.length },
    () => null,
  );
  const operations: Array<AgentControlTaskReconcileOperation> = [];
  const existingByIdentity = new Map<string, Array<number>>();
  const existingByNumber = new Map<string, Array<number>>();
  const existingByIssueNode = new Map<string, Array<number>>();
  const issuesByIdentity = new Map<string, Array<number>>();
  const issuesByNumber = new Map<string, Array<number>>();
  const issuesByIssueNode = new Map<string, Array<number>>();
  const assignments = new Map<number, Assignment>();
  let classifiable = true;

  for (const [index, task] of existingTasks.entries()) {
    addToMultiMap(
      existingByIdentity,
      identityKey(task.source.repositoryNodeId, task.source.issueNodeId),
      index,
    );
    addToMultiMap(
      existingByNumber,
      numberKey(task.source.repositoryNodeId, task.source.issueNumber),
      index,
    );
    addToMultiMap(existingByIssueNode, task.source.issueNodeId, index);
  }
  for (const indexes of [...existingByIdentity.values(), ...existingByNumber.values()]) {
    if (indexes.length <= 1) continue;
    classifiable = false;
    for (const index of indexes) {
      taskClassifications[index] = {
        kind: "unclassifiable",
        task: existingTasks[index]!,
      };
    }
  }

  for (const [index, issue] of snapshot.issues.entries()) {
    addToMultiMap(issuesByIdentity, identityKey(issue.repositoryNodeId, issue.issueNodeId), index);
    addToMultiMap(issuesByNumber, numberKey(issue.repositoryNodeId, issue.number), index);
    addToMultiMap(issuesByIssueNode, issue.issueNodeId, index);
  }
  for (const indexes of [
    ...issuesByIdentity.values(),
    ...issuesByNumber.values(),
    ...issuesByIssueNode.values(),
  ]) {
    if (indexes.length <= 1) continue;
    classifiable = false;
    for (const index of indexes) {
      issueClassifications[index] = {
        kind: "unclassifiable",
        issue: snapshot.issues[index]!,
      };
    }
  }

  const assignTask = (taskIndex: number, assignment: Assignment) => {
    const previous = assignments.get(taskIndex);
    if (previous === undefined) {
      assignments.set(taskIndex, assignment);
      return true;
    }
    classifiable = false;
    issueClassifications[previous.issueIndex] = {
      kind: "unclassifiable",
      issue: snapshot.issues[previous.issueIndex]!,
    };
    issueClassifications[assignment.issueIndex] = {
      kind: "unclassifiable",
      issue: snapshot.issues[assignment.issueIndex]!,
    };
    taskClassifications[taskIndex] = {
      kind: "unclassifiable",
      task: existingTasks[taskIndex]!,
    };
    return false;
  };

  for (const [issueIndex, issue] of snapshot.issues.entries()) {
    if (issueClassifications[issueIndex] !== null) continue;
    const canonicalUpdatedAt = canonicalAgentControlTaskSourceTimestamp(issue.updatedAt);
    if (
      issue.repositoryNodeId !== snapshot.sourcePrecondition.repositoryNodeId ||
      canonicalUpdatedAt === null
    ) {
      classifiable = false;
      issueClassifications[issueIndex] = { kind: "unclassifiable", issue };
      continue;
    }

    const identityMatches =
      existingByIdentity.get(identityKey(issue.repositoryNodeId, issue.issueNodeId)) ?? [];
    const numberMatches =
      existingByNumber.get(numberKey(issue.repositoryNodeId, issue.number)) ?? [];
    const nodeMatches = existingByIssueNode.get(issue.issueNodeId) ?? [];
    if (identityMatches.length > 1 || numberMatches.length > 1 || nodeMatches.length > 1) {
      classifiable = false;
      issueClassifications[issueIndex] = { kind: "unclassifiable", issue };
      continue;
    }

    const exactIndex = identityMatches[0];
    const conflictIndexes = new Set<number>();
    if (exactIndex !== undefined) {
      const exact = existingTasks[exactIndex]!;
      if (exact.source.issueNumber !== issue.number || exact.source.issueUrl !== issue.url) {
        conflictIndexes.add(exactIndex);
      }
    }
    const numberIndex = numberMatches[0];
    if (numberIndex !== undefined && numberIndex !== exactIndex) conflictIndexes.add(numberIndex);
    const nodeIndex = nodeMatches[0];
    if (
      nodeIndex !== undefined &&
      existingTasks[nodeIndex]!.source.repositoryNodeId !== issue.repositoryNodeId
    ) {
      conflictIndexes.add(nodeIndex);
    }

    if (conflictIndexes.size > 1) {
      classifiable = false;
      issueClassifications[issueIndex] = { kind: "unclassifiable", issue };
      for (const taskIndex of conflictIndexes) {
        taskClassifications[taskIndex] = {
          kind: "unclassifiable",
          task: existingTasks[taskIndex]!,
        };
      }
      continue;
    }
    const conflictIndex = conflictIndexes.values().next().value as number | undefined;
    if (conflictIndex !== undefined) {
      if (!assignTask(conflictIndex, { issueIndex, kind: "local-conflict" })) continue;
      issueClassifications[issueIndex] = {
        kind: "local-conflict",
        issue,
        task: existingTasks[conflictIndex]!,
      };
      continue;
    }
    if (exactIndex !== undefined) {
      if (!assignTask(exactIndex, { issueIndex, kind: "exact-match" })) continue;
      issueClassifications[issueIndex] = {
        kind: "exact-match",
        issue,
        task: existingTasks[exactIndex]!,
      };
      continue;
    }

    const gate = sourceGate(issue);
    issueClassifications[issueIndex] =
      gate === "eligible" ? { kind: "new-eligible", issue } : { kind: "ineligible", issue };
  }

  for (const [taskIndex, task] of existingTasks.entries()) {
    if (taskClassifications[taskIndex]?.kind === "unclassifiable") continue;
    const assignment = assignments.get(taskIndex);
    if (assignment?.kind === "local-conflict") {
      taskClassifications[taskIndex] = { kind: "local-conflict", task };
      operations.push(
        task.sourceGate === "identity-invalid"
          ? { type: "unchanged" }
          : { type: "mark-identity-invalid", task },
      );
      continue;
    }
    if (assignment?.kind === "exact-match") {
      const issue = snapshot.issues[assignment.issueIndex]!;
      const canonicalUpdatedAt = canonicalAgentControlTaskSourceTimestamp(issue.updatedAt);
      if (canonicalUpdatedAt === null) {
        classifiable = false;
        taskClassifications[taskIndex] = { kind: "unclassifiable", task };
        issueClassifications[assignment.issueIndex] = { kind: "unclassifiable", issue };
        continue;
      }
      const gate = sourceGate(issue);
      const nextSnapshot = sourceSnapshot(issue, canonicalUpdatedAt);
      const timestampOrder = compareAgentControlTaskSourceTimestamps(
        canonicalUpdatedAt,
        task.sourceUpdatedAt,
      );
      if (
        timestampOrder === null ||
        timestampOrder < 0 ||
        compareAgentControlTaskSourceTimestamps(
          task.sourceUpdatedAt,
          task.sourceSnapshot.updatedAt,
        ) !== 0
      ) {
        classifiable = false;
        taskClassifications[taskIndex] = { kind: "unclassifiable", task };
        issueClassifications[assignment.issueIndex] = { kind: "unclassifiable", issue };
        continue;
      }
      if (task.sourceGate === "identity-invalid") {
        taskClassifications[taskIndex] = { kind: "unchanged", task };
        operations.push({ type: "unchanged" });
      } else if (task.status === "needs-attention" && task.sourceGate === "source-missing") {
        if (
          gate === "eligible" &&
          snapshot.sourcePrecondition.githubIntakeSequence > task.githubIntakeSequence
        ) {
          taskClassifications[taskIndex] = { kind: "exact-match", task, issue };
          operations.push({
            type: "recover-source-missing",
            task,
            issue,
            snapshot: nextSnapshot,
            sourceUpdatedAt: canonicalUpdatedAt,
          });
        } else {
          taskClassifications[taskIndex] = { kind: "unchanged", task };
          operations.push({ type: "unchanged" });
        }
      } else if (
        task.sourceGate === gate &&
        task.githubIntakeSequence === snapshot.sourcePrecondition.githubIntakeSequence &&
        sameTaskSourceSnapshot(task.sourceSnapshot, nextSnapshot)
      ) {
        taskClassifications[taskIndex] = { kind: "unchanged", task };
        operations.push({ type: "unchanged" });
      } else {
        taskClassifications[taskIndex] = { kind: "exact-match", task, issue };
        operations.push({
          type: "refresh",
          task,
          issue,
          gate,
          snapshot: nextSnapshot,
          sourceUpdatedAt: canonicalUpdatedAt,
        });
      }
      continue;
    }

    const exactPresent = issuesByIdentity.has(
      identityKey(task.source.repositoryNodeId, task.source.issueNodeId),
    );
    const nodePresent = issuesByIssueNode.has(task.source.issueNodeId);
    const numberPresent = issuesByNumber.has(
      numberKey(task.source.repositoryNodeId, task.source.issueNumber),
    );
    if (exactPresent || nodePresent || numberPresent) {
      classifiable = false;
      taskClassifications[taskIndex] = { kind: "unclassifiable", task };
      continue;
    }
    if (task.sourceGate === "identity-invalid") {
      taskClassifications[taskIndex] = { kind: "unchanged", task };
      operations.push({ type: "unchanged" });
      continue;
    }
    taskClassifications[taskIndex] = { kind: "source-missing", task };
    operations.push(
      task.status === "needs-attention" && task.sourceGate === "source-missing"
        ? { type: "unchanged" }
        : { type: "mark-source-missing", task },
    );
  }

  for (const [issueIndex, classification] of issueClassifications.entries()) {
    if (classification === null) {
      classifiable = false;
      issueClassifications[issueIndex] = {
        kind: "unclassifiable",
        issue: snapshot.issues[issueIndex]!,
      };
      continue;
    }
    if (classification.kind === "new-eligible") {
      const canonicalUpdatedAt = canonicalAgentControlTaskSourceTimestamp(
        classification.issue.updatedAt,
      );
      if (canonicalUpdatedAt === null) {
        classifiable = false;
        issueClassifications[issueIndex] = {
          kind: "unclassifiable",
          issue: classification.issue,
        };
      } else {
        operations.push({
          type: "create",
          issue: classification.issue,
          gate: "eligible",
          snapshot: sourceSnapshot(classification.issue, canonicalUpdatedAt),
          sourceUpdatedAt: canonicalUpdatedAt,
        });
      }
    } else if (classification.kind === "ineligible") {
      operations.push({ type: "unchanged" });
    }
  }

  return {
    classifiable,
    issueClassifications:
      issueClassifications as ReadonlyArray<AgentControlTaskIssueClassification>,
    taskClassifications:
      taskClassifications as ReadonlyArray<AgentControlTaskExistingClassification>,
    operations,
  };
};
