import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { epicSourceFingerprint, makeEpicInspector } from "./githubEpicSource.ts";
import { GithubIssueTrackerClientError } from "./Services/GithubIssueTrackerClient.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const repository = { repositoryNodeId: "repo-id", nameWithOwner: "owner/repo" };
const rawIssue = (number: number, subIssues = 0, dependencies = 0) => ({
  node_id: `issue-${number}`,
  number,
  repository_url: "https://api.github.com/repos/owner/repo",
  html_url: `https://github.com/owner/repo/issues/${number}`,
  title: `Issue ${number}`,
  state: "open",
  sub_issues_summary: { total: subIssues },
  issue_dependencies_summary: { total_blocked_by: dependencies },
});

const fixture = (
  options: {
    readonly root?: ReturnType<typeof rawIssue>;
    readonly children?: ReadonlyArray<ReturnType<typeof rawIssue>>;
    readonly dependencies?: Readonly<Record<number, ReadonlyArray<ReturnType<typeof rawIssue>>>>;
    readonly maxPages?: number;
    readonly transform?: (path: string, value: unknown, call: number) => unknown;
  } = {},
) => {
  const root = options.root ?? rawIssue(1, 3);
  const children = options.children ?? [rawIssue(3, 0, 1), rawIssue(2), rawIssue(4)];
  const dependencies = options.dependencies ?? { 3: [rawIssue(2)] };
  const calls: Array<ReadonlyArray<string>> = [];
  const perPathCalls = new Map<string, number>();
  const inspect = makeEpicInspector({
    resolveRepository: ({ locator }) =>
      Effect.succeed({
        repositoryNodeId: locator.name === "repo" ? "repo-id" : "other-repo-id",
        nameWithOwner: `${locator.owner}/${locator.name}`,
      }),
    pageSize: 2,
    maxPages: options.maxPages ?? 10,
    execute: (_cwd, args) => {
      calls.push(args);
      const path = args[3]!;
      const call = (perPathCalls.get(path) ?? 0) + 1;
      perPathCalls.set(path, call);
      const page = Number(args.find((arg) => arg.startsWith("page="))?.slice(5) ?? 1);
      const pageOf = (items: ReadonlyArray<unknown>) => items.slice((page - 1) * 2, page * 2);
      let value: unknown;
      if (path === "repos/owner/repo/issues/1") value = root;
      else if (path.endsWith("/sub_issues")) value = pageOf(children);
      else if (path.endsWith("/dependencies/blocked_by"))
        value = pageOf(dependencies[Number(path.split("/")[4])] ?? []);
      else
        return Effect.fail(
          new GithubIssueTrackerClientError({
            code: "github-command-failed",
            operation: "inspect-epic",
          }),
        );
      return Effect.succeed(encodeJson(options.transform?.(path, value, call) ?? value));
    },
  });
  return {
    calls,
    run: inspect({
      cwd: "/isolated/project",
      locator: { owner: "owner", name: "repo" },
      expectedRepository: repository,
      epicNumber: 1,
    }),
  };
};

describe("native GitHub epic source", () => {
  it.effect(
    "preserves native order, reads all pages and explicit dependencies without interpreting body links",
    () =>
      Effect.gen(function* () {
        const { run, calls } = fixture({
          transform: (_path, value) =>
            Array.isArray(value)
              ? value.map((issue) => ({ ...issue, body: "- [ ] #999\nDepends on #888" }))
              : value,
        });
        const source = yield* run;
        expect(source.tasks.map((task) => task.issue.number)).toEqual([3, 2, 4]);
        expect(source.tasks.map((task) => task.position)).toEqual([0, 1, 2]);
        expect(source.tasks[0]?.dependencies.map((issue) => issue.number)).toEqual([2]);
        expect(source.blockers).toEqual([]);
        expect(source.tasks[0]?.issue).not.toHaveProperty("eligible");
        expect(calls.every((args) => args.slice(0, 3).join(" ") === "api --method GET")).toBe(true);
        expect(calls.some((args) => args.includes("page=2"))).toBe(true);
      }),
  );

  it.effect(
    "records closed members and closed external prerequisites without claiming verification",
    () =>
      Effect.gen(function* () {
        const source = yield* fixture({
          children: [rawIssue(3, 0, 1), { ...rawIssue(2), state: "closed" }, rawIssue(4)],
          dependencies: { 3: [{ ...rawIssue(99), state: "closed" }] },
        }).run;
        expect(source.blockers).toEqual([]);
        expect(source.tasks[1]?.issue.state).toBe("closed");
        expect(source.tasks[0]?.dependencies[0]?.state).toBe("closed");
        expect(encodeJson(source)).not.toContain("verified");
      }),
  );

  it.effect(
    "reports an open prerequisite outside the epic without discarding an independent task",
    () =>
      Effect.gen(function* () {
        const source = yield* fixture({ dependencies: { 3: [rawIssue(99)] } }).run;
        expect(source.blockers).toMatchObject([{ code: "missing-prerequisite", issueNumber: 3 }]);
        expect(source.tasks.find((task) => task.issue.number === 4)?.dependencies).toEqual([]);
      }),
  );

  it.effect("reports nested members and cross-repository structures explicitly", () =>
    Effect.gen(function* () {
      const source = yield* fixture({
        children: [
          rawIssue(3, 1, 1),
          rawIssue(2),
          {
            ...rawIssue(4),
            repository_url: "https://api.github.com/repos/owner/other",
            html_url: "https://github.com/owner/other/issues/4",
          },
        ],
      }).run;
      expect(source.blockers.map((blocker) => blocker.code)).toEqual([
        "nested-sub-issues",
        "cross-repository",
      ]);
    }),
  );

  it.effect("detects cycles between open tasks", () =>
    Effect.gen(function* () {
      const source = yield* fixture({
        children: [rawIssue(3, 0, 1), rawIssue(2, 0, 1), rawIssue(4)],
        dependencies: { 3: [rawIssue(2, 0, 1)], 2: [rawIssue(3, 0, 1)] },
      }).run;
      expect(source.blockers.map((blocker) => blocker.code)).toContain("dependency-cycle");
    }),
  );

  for (const [name, options] of [
    ["missing child", { children: [rawIssue(3), rawIssue(2)] }],
    ["missing dependency", { dependencies: {} }],
    [
      "conflicting issue state across relationship responses",
      { dependencies: { 3: [{ ...rawIssue(2), state: "closed" }] } },
    ],
    ["duplicate member", { children: [rawIssue(3), rawIssue(3), rawIssue(4)] }],
    [
      "missing relationship summary",
      {
        transform: (path: string, value: unknown) =>
          path.endsWith("/sub_issues") ? [{ node_id: "unknown" }] : value,
      },
    ],
    [
      "membership changed while reading",
      {
        transform: (path: string, value: unknown, call: number) =>
          path.endsWith("/sub_issues") && call === 3 ? [rawIssue(7), rawIssue(2)] : value,
      },
    ],
    [
      "dependency changed with the same count",
      {
        transform: (path: string, value: unknown, call: number) =>
          path.includes("/3/dependencies/") && call === 2 ? [rawIssue(4)] : value,
      },
    ],
  ] as const) {
    it.effect(`rejects incomplete intake: ${name}`, () =>
      Effect.gen(function* () {
        const error = yield* fixture(options).run.pipe(Effect.flip);
        expect(error.code).toBe("github-decode-failed");
        expect(error.operation).toBe("inspect-epic");
      }),
    );
  }

  it.effect("rejects page limits instead of accepting partial success", () =>
    Effect.gen(function* () {
      const error = yield* fixture({ maxPages: 1 }).run.pipe(Effect.flip);
      expect(error.code).toBe("pagination-overflow");
    }),
  );

  it.effect("rejects a changed repository identity before reading an epic", () =>
    Effect.gen(function* () {
      let read = false;
      const inspect = makeEpicInspector({
        resolveRepository: () => Effect.succeed({ ...repository, repositoryNodeId: "replacement" }),
        execute: () => {
          read = true;
          return Effect.succeed("{}");
        },
        pageSize: 100,
        maxPages: 2,
      });
      const error = yield* inspect({
        cwd: "/project",
        locator: { owner: "owner", name: "repo" },
        expectedRepository: repository,
        epicNumber: 1,
      }).pipe(Effect.flip);
      expect(error.code).toBe("repository-identity-changed");
      expect(read).toBe(false);
    }),
  );

  it.effect("keeps inaccessible or missing issue reads in the error channel", () =>
    Effect.gen(function* () {
      const inspect = makeEpicInspector({
        resolveRepository: () => Effect.succeed(repository),
        execute: () =>
          Effect.fail(
            new GithubIssueTrackerClientError({
              code: "github-command-failed",
              operation: "inspect-epic",
            }),
          ),
        pageSize: 100,
        maxPages: 2,
      });
      const error = yield* inspect({
        cwd: "/project",
        locator: { owner: "owner", name: "repo" },
        expectedRepository: repository,
        epicNumber: 1,
      }).pipe(Effect.flip);
      expect(error.code).toBe("github-command-failed");
      expect(error.operation).toBe("inspect-epic");
    }),
  );

  it.effect("reports an empty closed epic as blocked", () =>
    Effect.gen(function* () {
      const source = yield* fixture({ root: { ...rawIssue(1), state: "closed" }, children: [] })
        .run;
      expect(source.blockers.map((blocker) => blocker.code)).toEqual(["closed-epic", "empty-epic"]);
    }),
  );

  it.effect(
    "fingerprints order, identity, dependency and state changes but excludes mutable prose and timestamps",
    () =>
      Effect.gen(function* () {
        const source = yield* fixture().run;
        expect(
          epicSourceFingerprint({ ...source.epic, title: "changed title" }, source.tasks),
        ).toBe(source.fingerprint);
        expect(epicSourceFingerprint(source.epic, source.tasks.toReversed())).not.toBe(
          source.fingerprint,
        );
        expect(
          epicSourceFingerprint(
            source.epic,
            source.tasks.map((task) => ({ ...task, dependencies: [] })),
          ),
        ).not.toBe(source.fingerprint);
        expect(epicSourceFingerprint({ ...source.epic, state: "closed" }, source.tasks)).not.toBe(
          source.fingerprint,
        );
      }),
  );
});
