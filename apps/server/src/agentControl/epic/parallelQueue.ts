import {
  AgentControlProjectPolicy,
  type AgentControlEpicPreview,
  type AgentControlEpicQueue,
  type AgentControlEpicQueueEntry,
  type AgentControlEpicProjectDependencyPlan,
  type AgentControlEpicRpcError,
  type AgentControlProjectState,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { epicDigest, epicError, loadProjectEpics, saveEpicRun } from "./authority.ts";
import { epicSourceChanges } from "./model.ts";
import { validateProjectDependencyPlan } from "./projectDependencyPlan.ts";
import { loadEpicQueue, saveEpicQueue } from "./queueAuthority.ts";
import { EpicHandoffRemote } from "./remote.ts";
import { createEpicRun, insertEpicRun } from "./runState.ts";

const decodePolicy = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlProjectPolicy));

/** The caller validates the acyclic project plan before ordering admissions. */
export const prioritizePendingEpics = (
  entries: readonly AgentControlEpicQueueEntry[],
  plan: AgentControlEpicProjectDependencyPlan,
) => {
  const owner = new Map(
    entries.flatMap((entry) =>
      entry.source.tasks.map((task) => [task.issue.issueNodeId, entry.entryId] as const),
    ),
  );
  const predecessors = new Map(entries.map((entry) => [entry.entryId, new Set<string>()]));
  for (const task of plan.tasks) {
    const id = owner.get(task.issueNodeId)!;
    const previous = predecessors.get(id)!;
    for (const dependency of task.dependsOn) {
      const prerequisite = owner.get(dependency)!;
      if (prerequisite !== id) previous.add(prerequisite);
    }
  }
  const depths = new Map<string, number>();
  const depth = (id: string): number => {
    const cached = depths.get(id);
    if (cached !== undefined) return cached;
    let result = 0;
    for (const prerequisite of predecessors.get(id)!)
      result = Math.max(result, 1 + depth(prerequisite));
    depths.set(id, result);
    return result;
  };
  return entries
    .filter((entry) => entry.status === "pending")
    .map((entry) => ({ entry, depth: depth(entry.entryId) }))
    .toSorted((a, b) => a.depth - b.depth)
    .map(({ entry }) => entry);
};

/** The existing project lock serializes admission; each admitted run retains its own authority. */
export const makeParallelEpicQueue = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* AgentControlEngine;
  const remote = yield* Effect.serviceOption(EpicHandoffRemote);
  const process = Effect.fn("ParallelEpicQueue.process")(function* (
    initialQueue: AgentControlEpicQueue,
    project: AgentControlProjectState,
    preview: (number: number) => Effect.Effect<AgentControlEpicPreview, AgentControlEpicRpcError>,
  ) {
    let queue = initialQueue;
    const { projectId } = queue;
    yield* validateProjectDependencyPlan(
      queue.entries,
      queue.projectDependencyPlan,
      queue.maxActiveEpics,
    );
    const now = yield* DateTime.now;
    const nextCheckAt = DateTime.formatIso(DateTime.add(now, { seconds: 60 }));
    let entries = [...queue.entries];
    const persist = Effect.fn("ParallelEpicQueue.persist")(function* (waitReason: string | null) {
      queue = yield* sql.withTransaction(
        saveEpicQueue(sql, queue, {
          ...queue,
          entries,
          nextEntryId: entries.find((entry) => entry.status === "pending")?.entryId ?? null,
          waitReason,
          nextCheckAt,
        }),
      );
    });
    const runs = yield* loadProjectEpics(sql, projectId);
    const projects = yield* sql<{
      cwd: string;
    }>`SELECT workspace_root AS cwd FROM main.projection_projects
      WHERE project_id=${projectId} AND deleted_at IS NULL`;
    const cwd = projects[0]?.cwd;
    // Review waits belong to the corresponding entry. They never stop other admissions.
    for (const entry of entries.filter((item) => item.status === "active")) {
      let run = runs.find((item) => item.epicRunId === entry.epicRunId);
      if (!run)
        return yield* epicError(
          "authority-conflict",
          "An active queue entry lost its Epic authority.",
        );
      let blockers = run.blockers;
      if (run.status === "succeeded") {
        const handoff = run.handoff;
        if (!handoff?.pullRequest) {
          blockers = [
            {
              code: "review-handoff",
              issueNumber: run.source.epic.number,
              message:
                "Verification passed. Explicitly publish the draft pull request for human review.",
            },
          ];
        } else if (Option.isSome(remote) && cwd) {
          const observation = yield* Effect.result(
            remote.value.readPullRequest({
              cwd,
              repository: handoff.repository,
              pullRequest: handoff.pullRequest,
            }),
          );
          if (observation._tag === "Failure") {
            blockers = [
              {
                code: observation.failure.code,
                issueNumber: run.source.epic.number,
                message: observation.failure.message,
              },
            ];
          } else {
            const observed = observation.success;
            if (epicDigest(observed) !== epicDigest(handoff.pullRequest)) {
              run = yield* sql.withTransaction(
                saveEpicRun(sql, run, {
                  handoff: {
                    ...handoff,
                    pullRequest: observed,
                    status: observed.state === "closed" ? "blocked" : "published",
                    error:
                      observed.state === "closed"
                        ? {
                            code: "pull-request-closed",
                            message: "The pull request was closed without merge.",
                          }
                        : null,
                    updatedAt: DateTime.formatIso(now),
                  },
                }),
              );
            }
            if (
              observed.state === "merged" &&
              observed.headSha === handoff.commitSha &&
              observed.baseBranch === handoff.targetBranch &&
              observed.mergeCommitSha
            ) {
              entries = entries.map((item) =>
                item.entryId === entry.entryId ? { ...item, status: "merged", blockers: [] } : item,
              );
              continue;
            }
            blockers = [
              {
                code: "awaiting-merge",
                issueNumber: run.source.epic.number,
                message:
                  "Waiting for human review and a confirmed merge of the verified result into its target branch.",
              },
            ];
          }
        } else {
          blockers = [
            {
              code: "source-unavailable",
              issueNumber: run.source.epic.number,
              message: "GitHub pull request observation or the project directory is unavailable.",
            },
          ];
        }
      } else if (run.status === "stopped") {
        blockers = [
          {
            code: "epic-stopped",
            issueNumber: run.source.epic.number,
            message:
              "This Epic was stopped; its history and reservations remain assigned to this run.",
          },
        ];
      }
      entries = entries.map((item) =>
        item.entryId === entry.entryId
          ? { ...item, status: run.status === "stopped" ? "stopped" : item.status, blockers }
          : item,
      );
    }
    // Completed review handoffs and stopped runs release execution slots, not their evidence.
    let occupied = runs.filter((run) =>
      ["running", "blocked", "verifying"].includes(run.status),
    ).length;
    const plan = queue.projectDependencyPlan!;
    const pending = prioritizePendingEpics(entries, plan);
    for (const entry of pending) {
      if (occupied >= (queue.maxActiveEpics ?? 1)) break;
      const inspected = yield* Effect.result(preview(entry.source.epic.number));
      if (inspected._tag === "Failure") {
        entries = entries.map((item) =>
          item.entryId === entry.entryId
            ? {
                ...item,
                blockers: [
                  {
                    code: inspected.failure.code,
                    issueNumber: entry.source.epic.number,
                    message: inspected.failure.message,
                  },
                ],
              }
            : item,
        );
        continue;
      }
      // Native prerequisites covered by the frozen graph are evaluated per task at execution.
      const coveredNumbers = new Set(
        entries.flatMap((item) => [
          item.source.epic.number,
          ...item.source.tasks.map((task) => task.issue.number),
        ]),
      );
      const blockers = inspected.success.blockers.filter(
        (blocker) =>
          blocker.code !== "missing-prerequisite" || !coveredNumbers.has(blocker.issueNumber ?? -1),
      );
      const frozen = yield* createEpicRun({
        projectId,
        commandId: entry.entryId,
        source: entry.source,
        checks: [],
        ...(entry.dependencyPlan ? { dependencyPlan: entry.dependencyPlan } : {}),
        parallelism: entry.parallelism ?? 1,
        projectDependencyPlan: plan,
      });
      blockers.push(
        ...epicSourceChanges(frozen, inspected.success.source).filter(
          (blocker) =>
            blocker.code !== "missing-prerequisite" ||
            !coveredNumbers.has(blocker.issueNumber ?? -1),
        ),
      );
      if (blockers.length) {
        entries = entries.map((item) =>
          item.entryId === entry.entryId ? { ...item, blockers } : item,
        );
        continue;
      }
      if (!cwd || Option.isNone(remote) || !remote.value.refreshQueueBase) {
        yield* persist("Fresh target-branch loading is unavailable on this server.");
        return;
      }
      const base = yield* Effect.result(
        remote.value.refreshQueueBase({ cwd, repository: entry.source.repository }),
      );
      if (base._tag === "Failure") {
        entries = entries.map((item) =>
          item.entryId === entry.entryId
            ? {
                ...item,
                blockers: [
                  {
                    code: base.failure.code,
                    issueNumber: entry.source.epic.number,
                    message: base.failure.message,
                  },
                ],
              }
            : item,
        );
        continue;
      }
      const policies = yield* sql<{
        policy: string;
      }>`SELECT policy_json AS policy FROM main.agent_control_project_policies WHERE project_id=${projectId}`;
      const policy = yield* decodePolicy(policies[0]?.policy ?? "{}");
      const run = yield* createEpicRun({
        projectId,
        commandId: entry.entryId,
        source: entry.source,
        checks: policy.verificationChecks ?? [],
        ...(entry.dependencyPlan ? { dependencyPlan: entry.dependencyPlan } : {}),
        parallelism: entry.parallelism ?? 1,
        projectDependencyPlan: plan,
        initialBase: base.success,
      });
      const admitted = yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* engine.getProjectState({ projectId });
          if (
            current.mode !== "armed" ||
            current.pausedFromMode !== null ||
            current.revision !== project.revision
          )
            return false;
          const latest = yield* loadEpicQueue(sql, projectId);
          if (latest?.revision !== queue.revision)
            return yield* epicError(
              "revision-conflict",
              "Queue approval changed before admission.",
            );
          yield* insertEpicRun(sql, run);
          entries = entries.map((item) =>
            item.entryId === entry.entryId
              ? { ...item, status: "active", epicRunId: run.epicRunId, blockers: [] }
              : item,
          );
          queue = yield* saveEpicQueue(sql, queue, { ...queue, entries, nextCheckAt });
          return true;
        }),
      );
      if (!admitted) return;
      occupied += 1;
    }
    yield* persist(
      entries.some((entry) => entry.status === "pending")
        ? "Waiting for an active Epic slot or the entry's prerequisites. Tasks share the host and provider capacity."
        : "Approved Epics execute independently; each result requires its own human review and merge.",
    );
  });
  return { process };
});
