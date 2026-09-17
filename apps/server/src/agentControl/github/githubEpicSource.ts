import * as NodeCrypto from "node:crypto";

import {
  type AgentControlEpicIssue,
  type AgentControlEpicSource,
  type AgentControlEpicSourceBlocker,
  type AgentControlEpicTaskSource,
  type AgentControlGithubRepositoryBinding,
  NonNegativeInt,
  PositiveInt,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  GithubIssueTrackerClientError,
  type GithubIssueTrackerClientShape,
} from "./Services/GithubIssueTrackerClient.ts";

// GitHub REST native sub-issue and dependency responses include these totals.
// Missing summaries cannot be interpreted as an empty relationship list.
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const RawIssue = Schema.Struct({
  node_id: TrimmedNonEmptyString,
  number: PositiveInt,
  repository_url: TrimmedNonEmptyString,
  html_url: TrimmedNonEmptyString,
  title: Schema.String,
  body: Schema.optionalKey(Schema.NullOr(Schema.String)),
  state: Schema.Literals(["open", "closed"]),
  pull_request: Schema.optionalKey(Schema.Unknown),
  sub_issues_summary: Schema.Struct({ total: NonNegativeInt }),
  issue_dependencies_summary: Schema.Struct({ total_blocked_by: NonNegativeInt }),
});
const decodeIssue = Schema.decodeUnknownEffect(Schema.fromJsonString(RawIssue));
const decodePage = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(RawIssue)));
const incomplete = () =>
  new GithubIssueTrackerClientError({ code: "github-decode-failed", operation: "inspect-epic" });

export const epicIssueContentFingerprint = (issue: {
  readonly title: string;
  readonly body?: string | null;
}) =>
  NodeCrypto.createHash("sha256")
    .update(encodeJson([issue.title, issue.body ?? null]))
    .digest("hex");

export const epicSourceFingerprint = (
  epic: AgentControlEpicIssue,
  tasks: ReadonlyArray<AgentControlEpicTaskSource>,
  dependencies: ReadonlyArray<AgentControlEpicIssue> = [],
) => {
  const identity = (issue: AgentControlEpicIssue) => [
    issue.repositoryNodeId,
    issue.issueNodeId,
    issue.number,
    issue.state,
    issue.subIssueCount,
    ...(issue.contentFingerprint === undefined ? [] : [issue.contentFingerprint]),
  ];
  return NodeCrypto.createHash("sha256")
    .update(
      encodeJson([
        identity(epic),
        tasks.map((task) => [
          task.position,
          identity(task.issue),
          task.dependencies
            .map(identity)
            .toSorted((a, b) => encodeJson(a).localeCompare(encodeJson(b))),
        ]),
        ...(dependencies.length > 0
          ? [
              dependencies
                .map(identity)
                .toSorted((a, b) => encodeJson(a).localeCompare(encodeJson(b))),
            ]
          : []),
      ]),
    )
    .digest("hex");
};

export const makeEpicInspector = (options: {
  readonly resolveRepository: GithubIssueTrackerClientShape["resolveRepository"];
  readonly execute: (
    cwd: string,
    args: ReadonlyArray<string>,
  ) => Effect.Effect<string, GithubIssueTrackerClientError>;
  readonly pageSize: number;
  readonly maxPages: number;
}): NonNullable<GithubIssueTrackerClientShape["inspectEpic"]> =>
  Effect.fn("GithubIssueTrackerClient.inspectEpic")(function* (input) {
    const repository = yield* options.resolveRepository(input);
    if (
      repository.repositoryNodeId !== input.expectedRepository.repositoryNodeId ||
      repository.nameWithOwner.toLowerCase() !==
        input.expectedRepository.nameWithOwner.toLowerCase()
    ) {
      return yield* new GithubIssueTrackerClientError({
        code: "repository-identity-changed",
        operation: "inspect-epic",
      });
    }
    const repositories = new Map<string, AgentControlGithubRepositoryBinding>([
      [repository.nameWithOwner.toLowerCase(), repository],
    ]);
    const observedIssues = new Map<string, string>();
    const normalize = Effect.fn("GithubIssueTrackerClient.normalizeEpicIssue")(function* (
      raw: typeof RawIssue.Type,
    ) {
      const match = /^https:\/\/api\.github\.com\/repos\/([^/]+)\/([^/]+)$/.exec(
        raw.repository_url,
      );
      const owner = match?.[1];
      const name = match?.[2];
      if (!owner || !name || raw.pull_request !== undefined) return yield* incomplete();
      const nameWithOwner = `${owner}/${name}`;
      if (
        raw.html_url.toLowerCase() !==
        `https://github.com/${nameWithOwner}/issues/${raw.number}`.toLowerCase()
      ) {
        return yield* incomplete();
      }
      let binding = repositories.get(nameWithOwner.toLowerCase());
      if (!binding) {
        binding = yield* options.resolveRepository({ cwd: input.cwd, locator: { owner, name } });
        repositories.set(nameWithOwner.toLowerCase(), binding);
      }
      const observed = encodeJson([
        binding.repositoryNodeId,
        raw.number,
        raw.state,
        raw.sub_issues_summary.total,
        epicIssueContentFingerprint(raw),
      ]);
      const previous = observedIssues.get(raw.node_id);
      if (previous !== undefined && previous !== observed) return yield* incomplete();
      observedIssues.set(raw.node_id, observed);
      return {
        repositoryNodeId: binding.repositoryNodeId,
        nameWithOwner: binding.nameWithOwner,
        issueNodeId: raw.node_id,
        number: raw.number,
        url: raw.html_url,
        title: raw.title,
        contentFingerprint: epicIssueContentFingerprint(raw),
        state: raw.state,
        subIssueCount: raw.sub_issues_summary.total,
      } satisfies AgentControlEpicIssue;
    });
    const get = (path: string) => options.execute(input.cwd, ["api", "--method", "GET", path]);
    const readIssue = (path: string) =>
      get(path).pipe(
        Effect.flatMap((raw) => decodeIssue(raw).pipe(Effect.mapError(() => incomplete()))),
      );
    const list = Effect.fn("GithubIssueTrackerClient.listEpicRelationships")(function* (
      path: string,
      expectedCount: number,
    ) {
      const items: Array<typeof RawIssue.Type> = [];
      const ids = new Set<string>();
      for (let page = 1; page <= options.maxPages; page += 1) {
        const raw = yield* options.execute(input.cwd, [
          "api",
          "--method",
          "GET",
          path,
          "-f",
          `per_page=${options.pageSize}`,
          "-f",
          `page=${page}`,
        ]);
        const decoded = yield* decodePage(raw).pipe(Effect.mapError(() => incomplete()));
        for (const item of decoded) {
          if (ids.has(item.node_id)) return yield* incomplete();
          ids.add(item.node_id);
          items.push(item);
        }
        if (items.length > expectedCount) return yield* incomplete();
        if (decoded.length < options.pageSize) {
          if (items.length !== expectedCount) return yield* incomplete();
          return items;
        }
      }
      return yield* new GithubIssueTrackerClientError({
        code: "pagination-overflow",
        operation: "inspect-epic",
      });
    });
    const rootPath = `repos/${input.locator.owner}/${input.locator.name}/issues/${input.epicNumber}`;
    const root = yield* readIssue(rootPath);
    const epic = yield* normalize(root);
    if (epic.repositoryNodeId !== repository.repositoryNodeId || epic.number !== input.epicNumber) {
      return yield* new GithubIssueTrackerClientError({
        code: "issue-repository-changed",
        operation: "inspect-epic",
      });
    }
    const children = yield* list(`${rootPath}/sub_issues`, epic.subIssueCount);
    const rawEpicDependencies = yield* list(
      `${rootPath}/dependencies/blocked_by`,
      root.issue_dependencies_summary.total_blocked_by,
    );
    const dependencies = yield* Effect.forEach(rawEpicDependencies, normalize);
    const blockers: AgentControlEpicSourceBlocker[] = [];
    for (const dependency of dependencies) {
      if (dependency.repositoryNodeId !== repository.repositoryNodeId)
        blockers.push({
          code: "cross-repository",
          issueNumber: epic.number,
          message: `Epic #${epic.number} depends on an issue in another repository. Only same-repository dependencies are supported.`,
        });
      else if (dependency.state === "open")
        blockers.push({
          code: "missing-prerequisite",
          issueNumber: epic.number,
          message: `Epic #${epic.number} waits for open prerequisite #${dependency.number}. Its native GitHub dependency must be satisfied before execution.`,
        });
    }
    if (epic.state === "closed")
      blockers.push({
        code: "closed-epic",
        issueNumber: epic.number,
        message: "The selected epic is already closed.",
      });
    if (children.length === 0)
      blockers.push({
        code: "empty-epic",
        issueNumber: epic.number,
        message: "The selected issue has no native GitHub sub-issues.",
      });
    const inspectedTasks = yield* Effect.forEach(
      children,
      Effect.fn("GithubIssueTrackerClient.inspectEpicTask")(function* (child, position) {
        const taskBlockers: AgentControlEpicSourceBlocker[] = [];
        const issue = yield* normalize(child);
        if (issue.subIssueCount > 0)
          taskBlockers.push({
            code: "nested-sub-issues",
            issueNumber: issue.number,
            message: `#${issue.number} contains nested sub-issues. Only one level is supported.`,
          });
        if (issue.repositoryNodeId !== repository.repositoryNodeId)
          taskBlockers.push({
            code: "cross-repository",
            issueNumber: issue.number,
            message: `#${issue.number} belongs to another repository. Only same-repository sub-issues are supported.`,
          });
        const rawDependencies = yield* list(
          `repos/${issue.nameWithOwner}/issues/${issue.number}/dependencies/blocked_by`,
          child.issue_dependencies_summary.total_blocked_by,
        );
        const dependencies = yield* Effect.forEach(rawDependencies, normalize);
        for (const dependency of dependencies) {
          if (dependency.repositoryNodeId !== repository.repositoryNodeId)
            taskBlockers.push({
              code: "cross-repository",
              issueNumber: issue.number,
              message: `#${issue.number} depends on an issue in another repository.`,
            });
          else if (
            dependency.state === "open" &&
            !children.some((member) => member.node_id === dependency.issueNodeId)
          )
            taskBlockers.push({
              code: "missing-prerequisite",
              issueNumber: issue.number,
              message: `#${issue.number} requires open issue #${dependency.number}, which is outside the selected epic.`,
            });
        }
        return { task: { issue, position, dependencies }, blockers: taskBlockers };
      }),
      { concurrency: 4 },
    );
    const tasks = inspectedTasks.map((entry) => entry.task);
    blockers.push(...inspectedTasks.flatMap((entry) => entry.blockers));
    // Closed issues are pre-existing prerequisites, never T3-verified results.
    const openTasks = new Map(
      tasks
        .filter((task) => task.issue.state === "open")
        .map((task) => [task.issue.issueNodeId, task]),
    );
    const visiting = new Set<string>();
    const visited = new Set<string>();
    const hasCycle = (id: string): boolean => {
      if (visiting.has(id)) return true;
      if (visited.has(id)) return false;
      visiting.add(id);
      for (const dependency of openTasks.get(id)?.dependencies ?? []) {
        if (openTasks.has(dependency.issueNodeId) && hasCycle(dependency.issueNodeId)) return true;
      }
      visiting.delete(id);
      visited.add(id);
      return false;
    };
    if ([...openTasks.keys()].some(hasCycle))
      blockers.push({
        code: "dependency-cycle",
        issueNumber: null,
        message: "The epic contains a cycle between open sub-issues.",
      });
    // Detect membership/order changes while reading the graph. GitHub offers no
    // transactional graph read; runtime revalidation also fences every start.
    const finalRoot = yield* readIssue(rootPath);
    const finalChildren = yield* list(`${rootPath}/sub_issues`, finalRoot.sub_issues_summary.total);
    if (
      finalRoot.node_id !== root.node_id ||
      finalRoot.state !== root.state ||
      epicIssueContentFingerprint(finalRoot) !== epicIssueContentFingerprint(root) ||
      finalRoot.issue_dependencies_summary.total_blocked_by !==
        root.issue_dependencies_summary.total_blocked_by ||
      encodeJson(
        finalChildren.map((issue) => [
          issue.node_id,
          issue.state,
          issue.sub_issues_summary.total,
          issue.issue_dependencies_summary.total_blocked_by,
          epicIssueContentFingerprint(issue),
        ]),
      ) !==
        encodeJson(
          children.map((issue) => [
            issue.node_id,
            issue.state,
            issue.sub_issues_summary.total,
            issue.issue_dependencies_summary.total_blocked_by,
            epicIssueContentFingerprint(issue),
          ]),
        )
    )
      return yield* incomplete();
    const refreshedEpicDependencies = yield* Effect.forEach(
      yield* list(`${rootPath}/dependencies/blocked_by`, dependencies.length),
      normalize,
    );
    if (
      epicSourceFingerprint(epic, [], dependencies) !==
      epicSourceFingerprint(epic, [], refreshedEpicDependencies)
    )
      return yield* incomplete();
    yield* Effect.forEach(
      tasks,
      Effect.fn("GithubIssueTrackerClient.revalidateEpicDependencies")(function* (task) {
        const dependencies = yield* list(
          `repos/${task.issue.nameWithOwner}/issues/${task.issue.number}/dependencies/blocked_by`,
          task.dependencies.length,
        );
        const refreshed = yield* Effect.forEach(dependencies, normalize);
        if (
          epicSourceFingerprint(epic, [task]) !==
          epicSourceFingerprint(epic, [{ ...task, dependencies: refreshed }])
        )
          return yield* incomplete();
      }),
      { concurrency: 4, discard: true },
    );
    return {
      format: "github-native-sub-issues-v1",
      repository,
      epic,
      dependencies,
      tasks,
      blockers,
      fingerprint: epicSourceFingerprint(epic, tasks, dependencies),
      inspectedAt: DateTime.formatIso(yield* DateTime.now),
    } satisfies AgentControlEpicSource;
  });
