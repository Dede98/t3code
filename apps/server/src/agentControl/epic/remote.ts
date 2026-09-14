import type {
  AgentControlEpicHandoffPullRequest,
  AgentControlWorktreeRepositoryIdentity,
} from "@t3tools/contracts";
import { normalizeGitRemoteUrl } from "@t3tools/shared/git";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { GitHubCli } from "../../sourceControl/GitHubCli.ts";
import { VcsProcess } from "../../vcs/VcsProcess.ts";

export class EpicHandoffRemoteError extends Schema.TaggedError<EpicHandoffRemoteError>()(
  "EpicHandoffRemoteError",
  { code: Schema.String, message: Schema.String },
) {}

export interface EpicHandoffRemoteAuthority {
  readonly cwd: string;
  readonly repositoryCommonDir: string;
  readonly repository: AgentControlWorktreeRepositoryIdentity;
  readonly targetBranch: string;
  readonly baseCommitSha: string;
  readonly commitSha: string;
  readonly branchName: string;
  readonly ownershipToken: string;
  readonly branchCreationAttempted?: boolean;
}
export interface EpicHandoffRemotePublishInput extends EpicHandoffRemoteAuthority {
  readonly branchCreationAttempted: boolean;
  readonly epicNumber: number;
  readonly childIssueNumbers: ReadonlyArray<number>;
  readonly childCheckCount: number;
  readonly finalCheckCount: number;
}
export class EpicHandoffRemote extends Context.Service<
  EpicHandoffRemote,
  {
    readonly prepare: (
      input: EpicHandoffRemoteAuthority,
      options?: { readonly initialPreview: boolean },
    ) => Effect.Effect<void, EpicHandoffRemoteError>;
    readonly publish: (
      input: EpicHandoffRemotePublishInput,
      hooks: {
        readonly beforeBranchCreate: () => Effect.Effect<void, EpicHandoffRemoteError>;
      },
    ) => Effect.Effect<AgentControlEpicHandoffPullRequest, EpicHandoffRemoteError>;
  }
>()("t3/agentControl/epic/remote/EpicHandoffRemote") {}

const failure = (code: string, message: string) => new EpicHandoffRemoteError({ code, message });
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeBody = (body: unknown) =>
  encodeJson(body).pipe(
    Effect.mapError(() =>
      failure("handoff-authority-invalid", "The publication request could not be encoded."),
    ),
  );
const objectId = /^[0-9a-f]{40}(?:[0-9a-f]{24})?$/;
const Repository = Schema.Struct({
  node_id: Schema.String,
  full_name: Schema.String,
  default_branch: Schema.String,
  permissions: Schema.optionalKey(Schema.Struct({ push: Schema.optionalKey(Schema.Boolean) })),
});
const PullRequest = Schema.Struct({
  number: Schema.Int,
  html_url: Schema.String,
  state: Schema.Literals(["open", "closed"]),
  merged_at: Schema.NullOr(Schema.String),
  draft: Schema.Boolean,
  body: Schema.NullOr(Schema.String),
  head: Schema.Struct({
    ref: Schema.String,
    sha: Schema.String,
    repo: Schema.NullOr(Schema.Struct({ node_id: Schema.String })),
  }),
  base: Schema.Struct({ ref: Schema.String, repo: Schema.Struct({ node_id: Schema.String }) }),
});
const CreatedRef = Schema.Struct({
  ref: Schema.String,
  object: Schema.Struct({ sha: Schema.String }),
});
const decodeRejection = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ message: Schema.String })),
);
const marker = (input: EpicHandoffRemoteAuthority) =>
  `<!-- t3auto-epic-handoff:${input.ownershipToken} -->`;
const tagRef = (input: EpicHandoffRemoteAuthority) =>
  `refs/tags/t3auto-handoff/${input.ownershipToken}`;
const tagContent = (input: EpicHandoffRemoteAuthority) =>
  `object ${input.commitSha}\ntype commit\ntag t3auto-handoff/${input.ownershipToken}\ntagger T3Auto <t3auto@localhost> 0 +0000\n\n${marker(input)}\nBranch: ${input.branchName}\nBase: ${input.baseCommitSha}\nRepository: ${input.repository.repositoryNodeId}\n`;

/** Only trusted numeric evidence and canonical links go into public GitHub text. */
export const buildEpicHandoffPullRequest = (input: EpicHandoffRemotePublishInput) => {
  const repositoryUrl = `https://github.com/${input.repository.nameWithOwner}`;
  return {
    title: `T3Auto Epic #${input.epicNumber}: verified combined result`,
    body: [
      "This draft contains the accepted combined result of a T3Auto Epic run for human review.",
      "",
      `Epic: ${repositoryUrl}/issues/${input.epicNumber}`,
      "",
      "Accepted sub-issues:",
      ...input.childIssueNumbers.map((number) => `- ${repositoryUrl}/issues/${number}`),
      "",
      `Verified result commit: \`${input.commitSha}\``,
      `Original base commit: \`${input.baseCommitSha}\``,
      "",
      `Accepted child verification: ${input.childCheckCount} passed required controller checks.`,
      `Final combined verification: ${input.finalCheckCount} passed required controller checks.`,
      "Verification covers the stated result and original base commits. Later target-branch changes are not included in these checks.",
      "",
      marker(input),
    ].join("\n"),
  };
};

export const makeEpicHandoffRemote = Effect.gen(function* () {
  const process = yield* VcsProcess;
  const github = yield* GitHubCli;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = Effect.fn("EpicHandoffRemote.git")(function* (
    input: EpicHandoffRemoteAuthority,
    args: ReadonlyArray<string>,
    stdin?: string,
  ) {
    return yield* process
      .run({
        operation: "EpicHandoffRemote.git",
        command: "git",
        cwd: input.cwd,
        args,
        ...(stdin === undefined ? {} : { stdin }),
        maxOutputBytes: 1_000_000,
      })
      .pipe(
        Effect.mapError(() =>
          failure(
            "remote-unavailable",
            "Git could not complete the handoff. Check the environment's repository access and retry.",
          ),
        ),
      );
  });
  const api = Effect.fn("EpicHandoffRemote.api")(function* (
    input: EpicHandoffRemoteAuthority,
    endpoint: string,
    body?: unknown,
    paginate = false,
  ) {
    return yield* github
      .execute({
        cwd: input.cwd,
        args: [
          "api",
          "--hostname",
          "github.com",
          endpoint,
          ...(body === undefined ? [] : ["--method", "POST", "--input", "-"]),
          ...(paginate ? ["--paginate", "--slurp"] : []),
        ],
        ...(body === undefined ? {} : { stdin: yield* encodeBody(body) }),
        maxOutputBytes: 1_000_000,
      })
      .pipe(
        Effect.mapError(() =>
          failure(
            "remote-unavailable",
            "GitHub could not confirm the handoff. Check the environment's GitHub access and retry to reconcile the remote state.",
          ),
        ),
      );
  });
  const createBranch = Effect.fn("EpicHandoffRemote.createBranch")(function* (
    input: EpicHandoffRemoteAuthority,
  ) {
    const result = yield* process
      .run({
        operation: "EpicHandoffRemote.createBranch",
        command: "gh",
        cwd: input.cwd,
        args: [
          "api",
          "--hostname",
          "github.com",
          `repos/${input.repository.nameWithOwner}/git/refs`,
          "--method",
          "POST",
          "--input",
          "-",
          "--include",
        ],
        stdin: yield* encodeBody({ ref: `refs/heads/${input.branchName}`, sha: input.commitSha }),
        allowNonZeroExit: true,
      })
      .pipe(
        Effect.mapError(() =>
          failure(
            "remote-unavailable",
            "GitHub did not confirm branch creation. Retry to reconcile the same handoff.",
          ),
        ),
      );
    const separator = /\r?\n\r?\n/.exec(result.stdout);
    const responseBody = separator
      ? result.stdout.slice(separator.index + separator[0].length)
      : "";
    if (result.exitCode !== 0) {
      const status = Number(
        /^HTTP\/[\d.]+ (\d{3})\b/m.exec(result.stdout)?.[1] ??
          /HTTP (\d{3})\b/.exec(result.stderr)?.[1],
      );
      if ([400, 401, 403, 404, 409, 422, 429].includes(status)) {
        // 422 also covers validation and spam protection. Retain the known
        // rejection before any further network work can lose that distinction.
        const rejection = decodeRejection(responseBody);
        const explicitCollision =
          status === 422 &&
          Option.isSome(rejection) &&
          rejection.value.message === "Reference already exists";
        if (explicitCollision)
          return yield* failure(
            "remote-branch-collision",
            "GitHub rejected creation of the occupied handoff branch. Nothing was overwritten.",
          );
        return yield* failure(
          "remote-branch-rejected",
          "GitHub rejected branch creation. Retry to check its validation or access restrictions and reconcile the same branch name.",
        );
      }
      return yield* failure(
        "remote-unavailable",
        "GitHub did not confirm branch creation. Retry to reconcile the same handoff.",
      );
    }
    if (!separator)
      return yield* failure(
        "remote-response-invalid",
        "GitHub returned an incomplete branch response. Retry to reconcile its state.",
      );
    return responseBody;
  });
  const decode = <S extends Schema.Constraint>(schema: S, raw: string) =>
    Schema.decodeEffect(Schema.fromJsonString(schema))(raw).pipe(
      Effect.mapError(() =>
        failure(
          "remote-response-invalid",
          "GitHub returned an incomplete handoff response. Retry to reconcile its state.",
        ),
      ),
    );
  const endpoint = (input: EpicHandoffRemoteAuthority, suffix = "") =>
    `repos/${input.repository.nameWithOwner}${suffix}`;

  const validate = Effect.fn("EpicHandoffRemote.validate")(function* (
    input: EpicHandoffRemoteAuthority,
    initialPreview = false,
  ) {
    if (
      !objectId.test(input.commitSha) ||
      !objectId.test(input.baseCommitSha) ||
      !/^[a-zA-Z0-9-]+\/[a-zA-Z0-9_.-]+$/.test(input.repository.nameWithOwner) ||
      !/^[a-zA-Z0-9-]{16,128}$/.test(input.ownershipToken) ||
      (!initialPreview && input.targetBranch === input.branchName)
    )
      return yield* failure("handoff-authority-invalid", "The saved handoff identity is invalid.");
    yield* git(input, ["check-ref-format", `refs/heads/${input.targetBranch}`]);
    yield* git(input, ["check-ref-format", `refs/heads/${input.branchName}`]);
    const common = (yield* git(input, ["rev-parse", "--git-common-dir"])).stdout.trim();
    const commonDir = yield* fs
      .realPath(path.resolve(input.cwd, common))
      .pipe(
        Effect.mapError(() =>
          failure("repository-identity-changed", "The original repository is no longer available."),
        ),
      );
    const info = yield* fs
      .stat(commonDir)
      .pipe(
        Effect.mapError(() =>
          failure("repository-identity-changed", "The original repository is no longer available."),
        ),
      );
    if (
      commonDir !== input.repositoryCommonDir ||
      info.dev !== input.repository.commonDirDevice ||
      Option.getOrUndefined(info.ino) !== input.repository.commonDirInode
    )
      return yield* failure(
        "repository-identity-changed",
        "The local repository identity changed since verification.",
      );
    const expectedKey = `github.com/${input.repository.nameWithOwner}`.toLowerCase();
    for (const mode of [[], ["--push"]]) {
      const urls = (yield* git(input, [
        "remote",
        "get-url",
        ...mode,
        "--all",
        input.repository.remoteName,
      ])).stdout
        .trim()
        .split("\n");
      if (
        urls.length !== 1 ||
        normalizeGitRemoteUrl(urls[0]!) !== expectedKey ||
        input.repository.canonicalKey !== expectedKey
      )
        return yield* failure(
          "repository-identity-changed",
          "The repository remote mapping changed since verification.",
        );
    }
    const defaultRef = (yield* git(input, [
      "symbolic-ref",
      "--quiet",
      `refs/remotes/${input.repository.remoteName}/HEAD`,
    ])).stdout.trim();
    if (
      defaultRef !== input.repository.defaultRemoteRef ||
      defaultRef !== `refs/remotes/${input.repository.remoteName}/${input.targetBranch}`
    )
      return yield* failure(
        "target-branch-changed",
        "The repository's target branch changed since verification.",
      );
    if (
      (yield* git(input, [
        "rev-parse",
        "--verify",
        `${input.commitSha}^{commit}`,
      ])).stdout.trim() !== input.commitSha
    )
      return yield* failure(
        "verified-commit-missing",
        "The accepted result commit is no longer available.",
      );
    yield* git(input, ["merge-base", "--is-ancestor", input.baseCommitSha, input.commitSha]);
    const repository = yield* decode(Repository, (yield* api(input, endpoint(input))).stdout);
    if (
      repository.node_id !== input.repository.repositoryNodeId ||
      repository.full_name.toLowerCase() !== input.repository.nameWithOwner.toLowerCase()
    )
      return yield* failure(
        "repository-identity-changed",
        "GitHub repository identity changed since verification.",
      );
    if (repository.default_branch !== input.targetBranch)
      return yield* failure(
        "target-branch-changed",
        "GitHub's target branch changed since verification.",
      );
    if (repository.permissions?.push !== true)
      return yield* failure(
        "github-write-denied",
        "The environment's GitHub account has no confirmed repository write permission.",
      );
  });
  const remoteRefs = Effect.fn("EpicHandoffRemote.remoteRefs")(function* (
    input: EpicHandoffRemoteAuthority,
  ) {
    // Use the validated URL, avoiding a different remote's push URL or refspec.
    const remoteUrl = (yield* git(input, [
      "remote",
      "get-url",
      "--push",
      "--all",
      input.repository.remoteName,
    ])).stdout.trim();
    if (
      remoteUrl.includes("\n") ||
      normalizeGitRemoteUrl(remoteUrl) !== input.repository.canonicalKey
    )
      return yield* failure(
        "repository-identity-changed",
        "The push destination changed since verification.",
      );
    const output = (yield* git(input, [
      "ls-remote",
      "--refs",
      remoteUrl,
      `refs/heads/${input.targetBranch}`,
      `refs/heads/${input.branchName}`,
      tagRef(input),
    ])).stdout;
    const refs = new Map<string, string>();
    for (const line of output.trim().split("\n").filter(Boolean)) {
      const [sha, ref] = line.split("\t");
      if (!sha || !objectId.test(sha) || !ref || refs.has(ref))
        return yield* failure(
          "remote-response-invalid",
          "Git returned ambiguous remote references.",
        );
      refs.set(ref, sha);
    }
    return { refs, remoteUrl };
  });
  const requireBase = (input: EpicHandoffRemoteAuthority, refs: ReadonlyMap<string, string>) =>
    refs.get(`refs/heads/${input.targetBranch}`) === input.baseCommitSha
      ? Effect.void
      : Effect.fail(
          failure(
            "target-base-changed",
            "The target branch moved since the Epic's original base. Its new state has not passed the combined verification.",
          ),
        );
  const findPullRequest = Effect.fn("EpicHandoffRemote.findPullRequest")(function* (
    input: EpicHandoffRemoteAuthority,
  ) {
    const owner = input.repository.nameWithOwner.split("/")[0]!;
    const pages = yield* decode(
      Schema.Array(Schema.Array(PullRequest)),
      (yield* api(
        input,
        endpoint(
          input,
          `/pulls?state=all&head=${encodeURIComponent(`${owner}:${input.branchName}`)}&per_page=100`,
        ),
        undefined,
        true,
      )).stdout,
    );
    const all = pages.flat();
    if (all.length > 1)
      return yield* failure(
        "pull-request-collision",
        "Multiple pull requests use this handoff branch. Resolve the ambiguity on GitHub.",
      );
    const pr = all[0];
    if (!pr) return null;
    if (
      pr.head.ref !== input.branchName ||
      pr.head.sha !== input.commitSha ||
      pr.base.ref !== input.targetBranch ||
      pr.head.repo?.node_id !== input.repository.repositoryNodeId ||
      pr.base.repo.node_id !== input.repository.repositoryNodeId ||
      !pr.body?.includes(marker(input)) ||
      pr.html_url !== `https://github.com/${input.repository.nameWithOwner}/pull/${pr.number}`
    )
      return yield* failure(
        "pull-request-collision",
        "The existing pull request does not match this verified Epic handoff.",
      );
    return {
      number: pr.number,
      url: pr.html_url,
      state: pr.merged_at !== null ? ("merged" as const) : pr.state,
      isDraft: pr.draft,
      headSha: pr.head.sha,
      baseBranch: pr.base.ref,
    };
  });
  const prepare = Effect.fn("EpicHandoffRemote.prepare")(function* (
    input: EpicHandoffRemoteAuthority,
    options?: { readonly initialPreview: boolean },
  ) {
    yield* validate(input, options?.initialPreview);
    const { refs } = yield* remoteRefs(input);
    if (options?.initialPreview) return yield* requireBase(input, refs);
    const tagObject = (yield* git(
      input,
      ["hash-object", "-t", "tag", "--stdin"],
      tagContent(input),
    )).stdout.trim();
    const branch = refs.get(`refs/heads/${input.branchName}`);
    const tag = refs.get(tagRef(input));
    if (
      (tag && tag !== tagObject) ||
      (branch &&
        (branch !== input.commitSha || tag !== tagObject || !input.branchCreationAttempted))
    )
      return yield* failure(
        "remote-branch-collision",
        "The handoff branch or ownership tag is occupied by another result.",
      );
    const existing = yield* findPullRequest(input);
    if (existing) {
      if (!input.branchCreationAttempted || tag !== tagObject)
        return yield* failure(
          "pull-request-collision",
          "Ownership of the existing pull request cannot be confirmed.",
        );
      return;
    }
    yield* requireBase(input, refs);
  });
  const publish: EpicHandoffRemote["Service"]["publish"] = Effect.fn("EpicHandoffRemote.publish")(
    function* (input, hooks) {
      yield* validate(input);
      const tagObject = (yield* git(
        input,
        ["hash-object", "-t", "tag", "--stdin"],
        tagContent(input),
      )).stdout.trim();
      let { refs, remoteUrl } = yield* remoteRefs(input);
      const branch = refs.get(`refs/heads/${input.branchName}`);
      const tag = refs.get(tagRef(input));
      if (
        (tag && tag !== tagObject) ||
        (branch &&
          (branch !== input.commitSha || tag !== tagObject || !input.branchCreationAttempted))
      )
        return yield* failure(
          "remote-branch-collision",
          "The remote handoff branch or ownership tag belongs to another operation or has a different commit. Nothing was overwritten.",
        );
      const existing = yield* findPullRequest(input);
      if (existing) {
        if (!input.branchCreationAttempted || tag !== tagObject)
          return yield* failure(
            "pull-request-collision",
            "Ownership of the existing pull request cannot be confirmed.",
          );
        return existing;
      }
      yield* requireBase(input, refs);
      if (!tag) {
        yield* git(input, ["mktag"], tagContent(input));
        // A normal tag push cannot update an existing tag. Upload only the immutable
        // object graph; GitHub's create-ref API creates the branch without any update.
        yield* git(input, [
          "-c",
          "push.followTags=false",
          "push",
          "--no-follow-tags",
          remoteUrl,
          `${tagObject}:${tagRef(input)}`,
        ]);
      }
      if (!branch) {
        yield* hooks.beforeBranchCreate();
        const created = yield* decode(CreatedRef, yield* createBranch(input));
        if (
          created.ref !== `refs/heads/${input.branchName}` ||
          created.object.sha !== input.commitSha
        )
          return yield* failure(
            "remote-branch-collision",
            "GitHub created an unexpected handoff reference.",
          );
      }
      refs = (yield* remoteRefs(input)).refs;
      if (
        refs.get(`refs/heads/${input.branchName}`) !== input.commitSha ||
        refs.get(tagRef(input)) !== tagObject
      )
        return yield* failure(
          "remote-branch-collision",
          "The remote handoff changed before pull request creation. Nothing was overwritten.",
        );
      yield* requireBase(input, refs);
      const recovered = yield* findPullRequest(input);
      if (recovered) return recovered;
      const text = buildEpicHandoffPullRequest(input);
      yield* api(input, endpoint(input, "/pulls"), {
        title: text.title,
        body: text.body,
        head: input.branchName,
        base: input.targetBranch,
        draft: true,
      });
      const result = yield* findPullRequest(input);
      if (!result)
        return yield* failure(
          "remote-unavailable",
          "GitHub has not yet confirmed the draft pull request. Retry to recover the same handoff.",
        );
      return result;
    },
  );
  return EpicHandoffRemote.of({ prepare, publish });
});
export const EpicHandoffRemoteLive = Layer.effect(EpicHandoffRemote, makeEpicHandoffRemote);
