import {
  ProjectId,
  type AgentControlEpicProjectDependencyPlan,
  type AgentControlEpicQueueEntry,
  type AgentControlProjectState,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { vi } from "vite-plus/test";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { loadProjectEpics } from "./authority.ts";
import { makeParallelEpicQueue, prioritizePendingEpics } from "./parallelQueue.ts";
import { loadEpicQueue, saveEpicQueue } from "./queueAuthority.ts";
import { EpicHandoffRemote } from "./remote.ts";

const at = "2026-09-18T09:00:00.000Z";
const projectId = ProjectId.make("parallel-priority");
const repository = { repositoryNodeId: "priority-repository", nameWithOwner: "owner/repo" };
const issue = (number: number) => ({
  ...repository,
  issueNodeId: `issue-${number}`,
  number,
  title: `Issue ${number}`,
  url: `https://github.com/owner/repo/issues/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});

/** Every task refers to all three tasks of every preceding Epic. */
const densePlan = (count: number) => {
  const entries: AgentControlEpicQueueEntry[] = Array.from({ length: count }, (_, index) => {
    const tasks = Array.from({ length: 3 }, (_, task) => ({
      issue: issue(index * 3 + task + 1),
      position: task,
      dependencies: [],
    }));
    const dependencies = Array.from(
      { length: index * 3 },
      (_, previous) => issue(previous + 1).issueNodeId,
    );
    const fingerprint = `scope-${index}`;
    return {
      entryId: `entry-${index}`,
      source: {
        format: "github-native-sub-issues-v1",
        repository,
        epic: issue(1000 + index),
        tasks,
        blockers: [],
        fingerprint,
        inspectedAt: at,
      },
      dependencyPlan: {
        version: 1,
        rationale: "Reviewed predecessor requirements.",
        sourceFingerprint: fingerprint,
        tasks: tasks.map((task) => ({
          issueNodeId: task.issue.issueNodeId,
          dependsOn: dependencies,
        })),
      },
      approvedAt: at,
      epicRunId: null,
      status: "pending",
      blockers: [],
    };
  });
  const plan: AgentControlEpicProjectDependencyPlan = {
    version: 1,
    rationale: "Reviewed dependencies across all approved scopes.",
    epics: entries.map((entry) => ({
      issueNodeId: entry.source.epic.issueNodeId,
      sourceFingerprint: entry.source.fingerprint,
    })),
    tasks: entries.flatMap((entry) => entry.dependencyPlan!.tasks),
  };
  return { entries, plan };
};

describe("parallel Epic admission priority", () => {
  for (const count of [8, 16, 32]) {
    it(`traverses a dense ${count}-Epic graph once and prioritizes its prerequisites`, () => {
      const { entries, plan } = densePlan(count);
      const reversed = entries.toReversed();
      // Counting synchronous graph lookups catches repeated recursive traversal
      // independently of machine speed. Restore the spy before assertions.
      const taskCount = count * 3;
      const epicEdges = (count * (count - 1)) / 2;
      const taskEdges = epicEdges * 9;
      // Two ownership/adjacency reads per task, one per task edge, and one
      // memoized walk over unique Epic edges and vertices. Sorting rereads none.
      const traversalBound = taskCount * 2 + taskEdges + epicEdges + count * 2;
      const originalGet = Map.prototype.get;
      let reads = 0;
      const lookups = vi.spyOn(Map.prototype, "get").mockImplementation(function (
        this: Map<unknown, unknown>,
        key: unknown,
      ) {
        if (++reads > traversalBound)
          throw new Error("Epic scheduling exceeded the bounded graph traversal");
        return originalGet.call(this, key);
      });
      let prioritized: AgentControlEpicQueueEntry[];
      try {
        prioritized = prioritizePendingEpics(reversed, plan);
      } finally {
        lookups.mockRestore();
      }
      assert.deepEqual(prioritized, entries);
      assert.deepEqual(reversed, entries.toReversed());
      assert.isAtMost(reads, traversalBound);
    });
  }

  it("preserves queue priority for equal depths and includes completed predecessors without readmitting them", () => {
    const { entries, plan } = densePlan(4);
    const completed: AgentControlEpicQueueEntry = {
      ...entries[0]!,
      status: "merged",
      epicRunId: "completed-root",
    };
    const parallel = {
      ...plan,
      tasks: plan.tasks.map((task, index) => ({
        ...task,
        // Epics 1 and 2 share only Epic 0; Epic 3 waits for both siblings.
        dependsOn: index >= 3 && index < 9 ? ["issue-1", "issue-2", "issue-3"] : task.dependsOn,
      })),
    };
    assert.deepEqual(
      prioritizePendingEpics([entries[3]!, entries[2]!, completed, entries[1]!], parallel),
      [entries[2], entries[1], entries[3]],
    );
  });

  it.effect(
    "admits prerequisite Epics from a reversed queue within the active limit and retains their identities on replay",
    () =>
      Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: 93 });
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,created_at,updated_at,scripts_json)
        VALUES (${projectId},'Priority test','/isolated/priority',${at},${at},'[]')`;
        const { entries, plan } = densePlan(8);
        const initial = yield* sql.withTransaction(
          saveEpicQueue(sql, null, {
            projectId,
            maxActiveEpics: 4,
            projectDependencyPlan: plan,
            revision: 0,
            entries: entries.toReversed(),
            nextEntryId: entries.at(-1)!.entryId,
            waitReason: null,
            nextCheckAt: null,
          }),
        );
        const project: AgentControlProjectState = {
          schemaVersion: 1,
          projectId,
          mode: "armed",
          pausedFromMode: null,
          revision: 1,
          sequence: 1,
          updatedAt: at,
        };
        const previewed: number[] = [];
        const queue = yield* makeParallelEpicQueue.pipe(
          Effect.provideService(AgentControlEngine, {
            getProjectState: () => Effect.succeed(project),
            dispatchHuman: () => Effect.die("Unexpected mode change"),
            dispatchController: () => Effect.die("Unexpected mode change"),
            dispatchSystem: () => Effect.die("Unexpected mode change"),
            streamDomainEvents: Stream.never,
          }),
          Effect.provideService(EpicHandoffRemote, {
            refreshQueueBase: () =>
              Effect.succeed({ commitSha: "a".repeat(40), targetBranch: "main" }),
            readPullRequest: () => Effect.die("Unexpected handoff observation"),
            prepare: () => Effect.die("Unexpected publication"),
            publish: () => Effect.die("Unexpected publication"),
          }),
        );
        const preview = (number: number) =>
          Effect.sync(() => {
            previewed.push(number);
            return {
              projectId,
              source: entries.find((entry) => entry.source.epic.number === number)!.source,
              canStart: true,
              blockers: [],
            };
          });
        yield* queue.process(initial, project, preview);
        assert.deepEqual(previewed, [1000, 1001, 1002, 1003]);
        const admitted = (yield* loadEpicQueue(sql, projectId))!;
        assert.deepEqual(
          admitted.entries
            .filter((entry) => entry.status === "active")
            .map((entry) => entry.entryId),
          ["entry-3", "entry-2", "entry-1", "entry-0"],
        );
        const runs = yield* loadProjectEpics(sql, projectId);
        assert.lengthOf(runs, 4);
        yield* queue.process(admitted, project, preview);
        assert.deepEqual(yield* loadProjectEpics(sql, projectId), runs);
        assert.deepEqual(previewed, [1000, 1001, 1002, 1003]);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});
