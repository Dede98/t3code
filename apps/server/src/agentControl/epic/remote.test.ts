// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import { VcsProcessSpawnError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { GitHubCli, GitHubCliCommandError } from "../../sourceControl/GitHubCli.ts";
import { VcsProcess, type VcsProcessOutput } from "../../vcs/VcsProcess.ts";
import { makeEpicHandoffRemote, type EpicHandoffRemotePublishInput } from "./remote.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRefRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ ref: Schema.String, sha: Schema.String })),
);
const decodePrRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      body: Schema.String,
      head: Schema.String,
      base: Schema.String,
      draft: Schema.Boolean,
    }),
  ),
);
const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const output = (stdout: string, exitCode = 0): VcsProcessOutput => ({
  stdout,
  stderr: "",
  exitCode: exitCode as VcsProcessOutput["exitCode"],
  stdoutTruncated: false,
  stderrTruncated: false,
});
const io = <A>(operation: () => Promise<A>) => Effect.tryPromise(operation);
const runGit = async (cwd: string, args: ReadonlyArray<string>, stdin?: string) => {
  if (stdin === undefined) return (await exec("git", [...args], { cwd })).stdout;
  return await new Promise<string>((resolve, reject) => {
    const child = NodeChildProcess.spawn("git", [...args], { cwd });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("exit", (code) => (code === 0 ? resolve(stdout) : reject(new Error(stderr))));
    child.stdin.end(stdin);
  });
};
const setup = Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    io(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "epic-handoff-remote-"))),
    (directory) => Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
  );
  const cwd = NodePath.join(directory, "local");
  const bare = NodePath.join(directory, "remote.git");
  yield* io(() => NodeFSP.mkdir(cwd));
  const git = (args: ReadonlyArray<string>) => io(async () => (await runGit(cwd, args)).trim());
  yield* git(["init", "-b", "main"]);
  yield* git(["config", "user.name", "Fixture"]);
  yield* git(["config", "user.email", "fixture@example.test"]);
  yield* io(() => NodeFSP.writeFile(NodePath.join(cwd, "file.txt"), "base\n"));
  yield* git(["add", "file.txt"]);
  yield* git(["commit", "-m", "base"]);
  const baseCommitSha = yield* git(["rev-parse", "HEAD"]);
  yield* git(["init", "--bare", bare]);
  yield* git(["push", bare, "main"]);
  yield* git(["remote", "add", "origin", "https://github.com/test-owner/test-repo.git"]);
  yield* git(["update-ref", "refs/remotes/origin/main", baseCommitSha]);
  yield* git(["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main"]);
  yield* io(() => NodeFSP.writeFile(NodePath.join(cwd, "file.txt"), "verified\n"));
  yield* git(["commit", "-am", "accepted"]);
  const commitSha = yield* git(["rev-parse", "HEAD"]);
  // HEAD and worktree both change after verification; neither may be published.
  yield* io(() => NodeFSP.writeFile(NodePath.join(cwd, "file.txt"), "later\n"));
  yield* git(["commit", "-am", "later"]);
  yield* io(() =>
    NodeFSP.writeFile(NodePath.join(cwd, "file.txt"), "dirty secret /Users/private/token\n"),
  );
  const repositoryCommonDir = yield* io(() => NodeFSP.realpath(NodePath.join(cwd, ".git")));
  const stat = yield* io(() => NodeFSP.stat(repositoryCommonDir));
  const input: EpicHandoffRemotePublishInput = {
    cwd,
    repositoryCommonDir,
    repository: {
      repositoryNodeId: "repo-node",
      nameWithOwner: "test-owner/test-repo",
      canonicalKey: "github.com/test-owner/test-repo",
      remoteName: "origin",
      remoteUrl: "github.com/test-owner/test-repo",
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: stat.dev,
      commonDirInode: stat.ino,
    },
    targetBranch: "main",
    baseCommitSha,
    commitSha,
    branchName: "t3auto/epic-2-123456789abcdef0",
    ownershipToken: "123456789abcdef0",
    branchCreationAttempted: false,
    epicNumber: 2,
    childIssueNumbers: [3, 4],
    childCheckCount: 2,
    finalCheckCount: 1,
  };
  type Pr = {
    number: number;
    html_url: string;
    state: "open" | "closed";
    merged_at: string | null;
    draft: boolean;
    body: string;
    head: { ref: string; sha: string; repo: { node_id: string } };
    base: { ref: string; repo: { node_id: string } };
  };
  const state = {
    attempted: false,
    tagPushes: 0,
    branchCreates: 0,
    prCreates: 0,
    prs: [] as Pr[],
    loseTagReply: false,
    loseBranchReply: false,
    losePrReply: false,
    denyWrite: false,
    repositoryId: "repo-node",
    collisionOnCreate: false,
    opaqueCollisionOnCreate: false,
    transientRejectionOnCreate: false,
    transientRejectionStatus: 422,
  };
  const failure = (command: string) =>
    new VcsProcessSpawnError({
      operation: "fixture",
      command,
      cwd,
      argumentCount: 0,
      cause: new Error("response lost /Users/private TOKEN"),
    });
  const process = VcsProcess.of({
    run: (request) =>
      Effect.tryPromise({
        try: async () => {
          if (request.command === "gh") {
            state.branchCreates++;
            if (state.collisionOnCreate)
              return output(
                `HTTP/2.0 422 Unprocessable Entity\n\n${encodeJson({ message: "Reference already exists" })}`,
                1,
              );
            if (state.transientRejectionOnCreate) {
              state.transientRejectionOnCreate = false;
              return output(
                `HTTP/2.0 ${state.transientRejectionStatus} Rejected\n\n${encodeJson({ message: "Request rejected." })}`,
                1,
              );
            }
            const body = decodeRefRequest(request.stdin!);
            await runGit(bare, [
              "update-ref",
              body.ref,
              body.sha,
              "0000000000000000000000000000000000000000",
            ]);
            if (state.opaqueCollisionOnCreate)
              return output("HTTP/2.0 422 Unprocessable Entity\n\n{}", 1);
            if (state.loseBranchReply) {
              state.loseBranchReply = false;
              throw new Error("lost");
            }
            return output(
              `HTTP/2.0 201 Created\n\n${encodeJson({ ref: body.ref, object: { sha: body.sha } })}`,
            );
          }
          const args = request.args.map((arg) =>
            arg === "https://github.com/test-owner/test-repo.git" ? bare : arg,
          );
          const stdout = await runGit(request.cwd, args, request.stdin);
          if (args.includes("push")) {
            state.tagPushes++;
            expect(args.some((arg) => arg.includes("--force"))).toBe(false);
            expect(args.at(-1)).toMatch(/^[0-9a-f]+:refs\/tags\/t3auto-handoff\//);
            if (state.loseTagReply) {
              state.loseTagReply = false;
              throw new Error("lost");
            }
          }
          return output(stdout);
        },
        catch: () => failure(request.command),
      }),
  });
  const github = GitHubCli.of({
    execute: (request) =>
      Effect.tryPromise({
        try: async () => {
          const endpoint = request.args[3]!;
          if (endpoint.includes("/pulls?")) return output(encodeJson([state.prs]));
          if (endpoint.endsWith("/pulls")) {
            state.prCreates++;
            const body = decodePrRequest(request.stdin!);
            expect(body.draft).toBe(true);
            state.prs.push({
              number: 10,
              html_url: "https://github.com/test-owner/test-repo/pull/10",
              state: "open",
              merged_at: null,
              draft: true,
              body: body.body,
              head: { ref: body.head, sha: commitSha, repo: { node_id: "repo-node" } },
              base: { ref: body.base, repo: { node_id: "repo-node" } },
            });
            if (state.losePrReply) {
              state.losePrReply = false;
              throw new Error("lost");
            }
            return output("{}");
          }
          return output(
            encodeJson({
              node_id: state.repositoryId,
              full_name: "test-owner/test-repo",
              default_branch: "main",
              permissions: { push: !state.denyWrite },
            }),
          );
        },
        catch: () => new GitHubCliCommandError({ command: "gh", cwd, cause: failure("gh") }),
      }),
    listOpenPullRequests: () => Effect.die("unused"),
    getPullRequest: () => Effect.die("unused"),
    getRepositoryCloneUrls: () => Effect.die("unused"),
    createRepository: () => Effect.die("unused"),
    createPullRequest: () => Effect.die("unused"),
    getDefaultBranch: () => Effect.die("unused"),
    checkoutPullRequest: () => Effect.die("unused"),
  });
  const remote = yield* makeEpicHandoffRemote.pipe(
    Effect.provideService(VcsProcess, process),
    Effect.provideService(GitHubCli, github),
  );
  const publish = () =>
    remote.publish(
      { ...input, branchCreationAttempted: state.attempted },
      {
        beforeBranchCreate: () =>
          Effect.sync(() => {
            state.attempted = true;
          }),
      },
    );
  const remoteGit = (args: ReadonlyArray<string>) =>
    io(async () => (await runGit(bare, args)).trim());
  return { input, state, remote, publish, git, remoteGit, cwd };
});

it.layer(NodeServices.layer)("Epic handoff remote", (it) => {
  it.effect("publishes exactly the verified commit with a draft and factual safe evidence", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      const result = yield* f.publish();
      expect(result.isDraft).toBe(true);
      expect(yield* f.remoteGit(["rev-parse", `refs/heads/${f.input.branchName}`])).toBe(
        f.input.commitSha,
      );
      expect(yield* f.git(["rev-parse", "HEAD"])).not.toBe(f.input.commitSha);
      expect(f.state.prs[0]!.body).toContain("2 passed required controller checks");
      expect(f.state.prs[0]!.body).toContain("/issues/3");
      expect(f.state.prs[0]!.body).not.toMatch(/secret|TOKEN|\/Users\/|closes|fixes|resolves/i);
      yield* f.publish();
      expect([f.state.tagPushes, f.state.branchCreates, f.state.prCreates]).toEqual([1, 1, 1]);
    }),
  );
  for (const boundary of ["loseTagReply", "loseBranchReply", "losePrReply"] as const) {
    it.effect(`recovers ${boundary} from actual remote state without duplicate effects`, () =>
      Effect.gen(function* () {
        const f = yield* setup;
        f.state[boundary] = true;
        expect((yield* Effect.flip(f.publish())).code).toBe("remote-unavailable");
        const result = yield* f.publish();
        expect(result.number).toBe(10);
        expect([f.state.tagPushes, f.state.branchCreates, f.state.prCreates]).toEqual([1, 1, 1]);
      }),
    );
  }
  it.effect("blocks foreign branches even at the accepted commit", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.git([
        "push",
        f.input.cwd.replace(/local$/, "remote.git"),
        `${f.input.commitSha}:refs/heads/${f.input.branchName}`,
      ]);
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-collision");
      expect(f.state.tagPushes).toBe(0);
      expect(f.state.prCreates).toBe(0);
    }),
  );
  it.effect("ignores unrelated preview-name branches before a publication intent exists", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.git([
        "push",
        f.input.cwd.replace(/local$/, "remote.git"),
        "HEAD:refs/heads/t3auto/preview",
      ]);
      yield* f.remote.prepare(
        { ...f.input, branchName: "t3auto/preview", ownershipToken: "preview-readonly-0000" },
        { initialPreview: true },
      );
      expect(f.state.prCreates).toBe(0);
    }),
  );
  it.effect("blocks a changed target base without remote mutations", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.git(["push", f.input.cwd.replace(/local$/, "remote.git"), "HEAD:refs/heads/main"]);
      expect((yield* Effect.flip(f.publish())).code).toBe("target-base-changed");
      expect(f.state.tagPushes).toBe(0);
    }),
  );
  it.effect("blocks an explicit create-ref collision without making a PR", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      f.state.collisionOnCreate = true;
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-collision");
      expect(f.state.prCreates).toBe(0);
    }),
  );
  it.effect("reconciles an opaque create-ref rejection as a collision when the branch exists", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      f.state.opaqueCollisionOnCreate = true;
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-rejected");
      f.state.attempted = false;
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-collision");
      expect(f.state.prCreates).toBe(0);
    }),
  );
  for (const status of [403, 422])
    it.effect(`retries a create-ref ${status} with the same uploaded result and branch`, () =>
      Effect.gen(function* () {
        const f = yield* setup;
        f.state.transientRejectionOnCreate = true;
        f.state.transientRejectionStatus = status;
        expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-rejected");
        f.state.attempted = false;
        expect(f.state.prCreates).toBe(0);
        expect(
          yield* f.remoteGit(["for-each-ref", "--format=%(refname)", "refs/heads/t3auto/"]),
        ).toBe("");
        expect((yield* f.publish()).headSha).toBe(f.input.commitSha);
        expect([f.state.tagPushes, f.state.branchCreates, f.state.prCreates]).toEqual([1, 2, 1]);
      }),
    );
  it.effect("blocks a foreign same-commit branch appearing after a rejected creation", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      f.state.transientRejectionOnCreate = true;
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-rejected");
      f.state.attempted = false;
      yield* f.remoteGit(["update-ref", `refs/heads/${f.input.branchName}`, f.input.commitSha]);
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-collision");
      expect([f.state.tagPushes, f.state.branchCreates, f.state.prCreates]).toEqual([1, 1, 0]);
    }),
  );
  for (const state of ["closed", "merged"] as const)
    it.effect(`finds an existing ${state} PR without replacement`, () =>
      Effect.gen(function* () {
        const f = yield* setup;
        yield* f.publish();
        f.state.prs[0]!.state = "closed";
        if (state === "merged") f.state.prs[0]!.merged_at = "2026-09-14T00:00:00Z";
        expect((yield* f.publish()).state).toBe(state);
        expect(f.state.prCreates).toBe(1);
      }),
    );
  for (const change of [
    "repository",
    "push-url",
    "common-dir",
    "target-branch",
    "write-permission",
  ] as const)
    it.effect(`blocks changed ${change}`, () =>
      Effect.gen(function* () {
        const f = yield* setup;
        if (change === "repository") f.state.repositoryId = "other";
        if (change === "push-url")
          yield* f.git([
            "remote",
            "set-url",
            "--push",
            "origin",
            "https://github.com/foreign/repo.git",
          ]);
        if (change === "target-branch")
          yield* f.git(["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/other"]);
        if (change === "write-permission") f.state.denyWrite = true;
        const error = yield* Effect.flip(
          change === "common-dir"
            ? f.remote.prepare({ ...f.input, repositoryCommonDir: "/wrong" })
            : f.publish(),
        );
        expect(error.code).toMatch(
          /repository-identity-changed|target-branch-changed|github-write-denied/,
        );
        expect(f.state.tagPushes).toBe(0);
      }),
    );
  it.effect("recovers a lost PR response after merge and target advance", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      f.state.losePrReply = true;
      yield* Effect.flip(f.publish());
      f.state.prs[0]!.state = "closed";
      f.state.prs[0]!.merged_at = "2026-09-14T00:00:00Z";
      yield* f.git(["push", f.input.cwd.replace(/local$/, "remote.git"), "HEAD:refs/heads/main"]);
      yield* f.remote.prepare({ ...f.input, branchCreationAttempted: true });
      expect((yield* f.publish()).state).toBe("merged");
      expect(f.state.prCreates).toBe(1);
    }),
  );
  it.effect("rejects foreign PR ownership and a different handoff intent", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.publish();
      f.state.prs[0]!.body = "Someone else's pull request";
      expect((yield* Effect.flip(f.publish())).code).toBe("pull-request-collision");
      const other = {
        ...f.input,
        ownershipToken: "fedcba0987654321",
        branchCreationAttempted: true,
      };
      expect(
        (yield* Effect.flip(f.remote.publish(other, { beforeBranchCreate: () => Effect.void })))
          .code,
      ).toBe("remote-branch-collision");
      expect(f.state.prCreates).toBe(1);
    }),
  );
  it.effect("blocks a changed published branch", () =>
    Effect.gen(function* () {
      const f = yield* setup;
      yield* f.publish();
      yield* f.git([
        "push",
        f.input.cwd.replace(/local$/, "remote.git"),
        `HEAD:refs/heads/${f.input.branchName}`,
      ]);
      expect((yield* Effect.flip(f.publish())).code).toBe("remote-branch-collision");
      expect(f.state.prCreates).toBe(1);
    }),
  );
});
