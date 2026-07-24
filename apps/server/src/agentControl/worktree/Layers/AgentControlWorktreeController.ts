import {
  CommandId,
  type AgentControlTaskId,
  type AgentControlWorktreeAttentionCode,
  type AgentControlWorktreeCommand,
  type AgentControlWorktreeReservationState,
  AgentControlWorktreeRpcError,
  type ProjectId,
} from "@t3tools/contracts";
import { normalizeGitRemoteUrl } from "@t3tools/shared/git";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import { GitVcsDriver } from "../../../vcs/GitVcsDriver.ts";
import { ServerConfig } from "../../../config.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseState,
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import {
  deriveAgentControlWorktreeBranchName,
  deriveAgentControlWorktreeReservationId,
  sha256FramedHex,
} from "../identity.ts";
import {
  deriveSafeAgentControlWorktreePath,
  validateExistingAgentControlWorktreePath,
} from "../pathSafety.ts";
import {
  AgentControlWorktreeController,
  type AgentControlWorktreeControllerShape,
} from "../Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktreeStateRepository } from "../Services/AgentControlWorktreeStateRepository.ts";

const GIT_OBJECT_ID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

const error = (
  code: AgentControlWorktreeRpcError["code"],
  operation: AgentControlWorktreeRpcError["operation"],
  projectId: ProjectId,
  taskId: AgentControlTaskId | null,
  reservationId: AgentControlWorktreeReservationState["reservationId"] | null = null,
) => new AgentControlWorktreeRpcError({ code, operation, projectId, taskId, reservationId });

const transitionCommandId = (
  base: CommandId,
  reservationId: AgentControlWorktreeReservationState["reservationId"],
  transition: string,
  revision: number,
) =>
  CommandId.make(
    `worktree-${sha256FramedHex([
      "agent-control-worktree-transition-command-v1",
      base,
      reservationId,
      transition,
      String(revision),
    ])}`,
  );

const parseWorktrees = (stdout: string) => {
  const entries: Array<{
    path: string;
    head: string | null;
    branch: string | null;
    detached: boolean;
  }> = [];
  let current: (typeof entries)[number] | null = null;
  for (const line of `${stdout}\n`.split("\n")) {
    if (line.startsWith("worktree ")) {
      current = {
        path: line.slice("worktree ".length),
        head: null,
        branch: null,
        detached: false,
      };
      entries.push(current);
    } else if (current && line.startsWith("HEAD ")) {
      current.head = line.slice("HEAD ".length);
    } else if (current && line.startsWith("branch refs/heads/")) {
      current.branch = line.slice("branch refs/heads/".length);
    } else if (current && line === "detached") {
      current.detached = true;
    } else if (line === "") {
      current = null;
    }
  }
  return entries;
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig;
  const git = yield* GitVcsDriver;
  const workflow = yield* GitWorkflowService;
  const guard = yield* AgentControlTaskConsumerGuard;
  const github = yield* AgentControlGithubStateRepository;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const engine = yield* AgentControlWorktreeEngine;
  const states = yield* AgentControlWorktreeStateRepository;
  const holderId = yield* leaseEngine.runtimeHolderId;
  const locks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

  const getLock = (commonDir: string) =>
    SynchronizedRef.modifyEffect(locks, (current) => {
      const existing = current.get(commonDir);
      if (existing) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => {
          const next = new Map(current);
          next.set(commonDir, lock);
          return [lock, next] as const;
        }),
      );
    });

  const preflight = Effect.fn("AgentControlWorktreeController.preflight")(function* (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
    operation: AgentControlWorktreeRpcError["operation"],
  ) {
    return yield* guard
      .useTaskConsumable(projectId, taskId, (task) =>
        Effect.gen(function* () {
          const projectRows = yield* sql<{
            readonly workspaceRoot: unknown;
            readonly deletedAt: unknown;
          }>`
            SELECT workspace_root AS "workspaceRoot", deleted_at AS "deletedAt"
            FROM projection_projects WHERE project_id = ${projectId}
          `.pipe(
            Effect.mapError(() =>
              error("internal-persistence-error", operation, projectId, taskId),
            ),
          );
          const project = projectRows[0];
          if (
            project === undefined ||
            project.deletedAt !== null ||
            typeof project.workspaceRoot !== "string" ||
            !path.isAbsolute(project.workspaceRoot)
          ) {
            return yield* error("project-unavailable", operation, projectId, taskId);
          }
          const githubState = yield* github
            .get(projectId)
            .pipe(
              Effect.mapError(() =>
                error("internal-persistence-error", operation, projectId, taskId),
              ),
            );
          if (Option.isNone(githubState) || githubState.value.config === null) {
            return yield* error("source-snapshot-unavailable", operation, projectId, taskId);
          }
          const sourceIdentityFingerprint =
            yield* deriveAgentControlSourceIdentityFingerprint(task);
          const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
            projectId,
            taskId,
            stageEvents,
            stageStates,
          ).pipe(
            Effect.mapError(() =>
              error("stage-run-projection-corrupt", operation, projectId, taskId),
            ),
          );
          const matches = stageHistory.filter(
            (stage) =>
              stage.taskRevision === task.revision &&
              stage.githubIntakeSequence === task.githubIntakeSequence &&
              stage.sourceIdentityFingerprint === sourceIdentityFingerprint &&
              stage.stageKind === "planning" &&
              stage.roleId === "planning" &&
              stage.stageOrdinal === 1 &&
              stage.attemptOrdinal === 1,
          );
          if (matches.length === 0) {
            return yield* error("stage-run-missing", operation, projectId, taskId);
          }
          if (matches.length !== 1) {
            return yield* error("stage-run-history-ambiguous", operation, projectId, taskId);
          }
          const stageRun = matches[0]!;
          if (stageRun.status !== "prepared") {
            return yield* error("stage-run-not-prepared", operation, projectId, taskId);
          }
          const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
          const lease = yield* loadAuthoritativeLeaseState(leaseId, leaseEvents, leaseStates).pipe(
            Effect.mapError(() => error("lease-projection-corrupt", operation, projectId, taskId)),
          );
          if (Option.isNone(lease)) {
            return yield* error("lease-missing", operation, projectId, taskId);
          }
          const leaseState = lease.value.state;
          if (leaseState.status !== "reserved") {
            return yield* error("lease-not-reserved", operation, projectId, taskId);
          }
          if (leaseState.holderId !== holderId) {
            return yield* error("lease-foreign-runtime", operation, projectId, taskId);
          }
          if (
            leaseState.leaseId !== leaseId ||
            leaseState.projectId !== projectId ||
            leaseState.taskId !== taskId ||
            leaseState.stageRunId !== stageRun.stageRunId ||
            leaseState.attemptId !== stageRun.attemptId ||
            leaseState.taskRevision !== task.revision ||
            leaseState.githubIntakeSequence !== task.githubIntakeSequence ||
            leaseState.sourceIdentityFingerprint !== sourceIdentityFingerprint
          ) {
            return yield* error("source-snapshot-stale", operation, projectId, taskId);
          }
          const expiresAt = canonicalTimestampMillis(leaseState.expiresAt);
          const now = yield* DateTime.now;
          if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
            return yield* error("lease-expired", operation, projectId, taskId);
          }
          return {
            task,
            projectWorkspace: project.workspaceRoot,
            repository: githubState.value.config.repository,
            sourceIdentityFingerprint,
            stageRun,
            lease: leaseState,
          };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          cause._tag === "AgentControlWorktreeRpcError"
            ? cause
            : error(
                cause._tag === "AgentControlTaskConsumerGuardError" &&
                  cause.reason === "project-unavailable"
                  ? "project-unavailable"
                  : cause._tag === "AgentControlTaskConsumerGuardError" &&
                      cause.reason === "task-missing"
                    ? "task-missing"
                    : cause._tag === "AgentControlTaskConsumerGuardError" &&
                        cause.reason === "task-status-inactive"
                      ? "task-not-candidate"
                      : cause._tag === "AgentControlTaskConsumerGuardError" &&
                          cause.reason === "task-source-ineligible"
                        ? "task-ineligible"
                        : cause._tag === "AgentControlTaskConsumerGuardError" &&
                            cause.reason === "task-stage-inactive"
                          ? "task-stage-inactive"
                          : cause._tag === "AgentControlTaskConsumerGuardError" &&
                              cause.reason === "source-snapshot-unavailable"
                            ? "source-snapshot-unavailable"
                            : cause._tag === "AgentControlTaskConsumerGuardError" &&
                                cause.reason === "internal-persistence-error"
                              ? "internal-persistence-error"
                              : "source-snapshot-stale",
                operation,
                projectId,
                taskId,
              ),
        ),
      );
  });

  const gitRun = (
    operation: string,
    cwd: string,
    args: ReadonlyArray<string>,
    allowNonZeroExit = false,
  ) =>
    git.execute({
      operation,
      cwd,
      args,
      allowNonZeroExit,
      timeoutMs: 10_000,
      maxOutputBytes: 1_000_000,
    });

  const resolveRepository = Effect.fn("AgentControlWorktreeController.resolveRepository")(
    function* (
      canonical: Effect.Success<ReturnType<typeof preflight>>,
      operation: AgentControlWorktreeRpcError["operation"],
    ) {
      const projectId = canonical.task.source.projectId;
      const taskId = canonical.task.taskId;
      const topLevelResult = yield* gitRun(
        "AgentControlWorktree.repository.topLevel",
        canonical.projectWorkspace,
        ["rev-parse", "--show-toplevel"],
      ).pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const repositoryWorkspace = topLevelResult.stdout.trim();
      if (!path.isAbsolute(repositoryWorkspace)) {
        return yield* error("repository-unavailable", operation, projectId, taskId);
      }
      const [canonicalRepository, canonicalProjectWorkspace] = yield* Effect.all([
        fs.realPath(repositoryWorkspace),
        fs.realPath(canonical.projectWorkspace),
      ]).pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const relativeProject = path.relative(canonicalRepository, canonicalProjectWorkspace);
      if (relativeProject === ".." || relativeProject.startsWith(`..${path.sep}`)) {
        return yield* error("repository-unavailable", operation, projectId, taskId);
      }
      const commonResult = yield* gitRun(
        "AgentControlWorktree.repository.commonDir",
        canonical.projectWorkspace,
        ["rev-parse", "--git-common-dir"],
      ).pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const commonCandidate = path.isAbsolute(commonResult.stdout.trim())
        ? commonResult.stdout.trim()
        : path.resolve(canonical.projectWorkspace, commonResult.stdout.trim());
      const repositoryCommonDir = yield* fs
        .realPath(commonCandidate)
        .pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const remoteNames = (yield* gitRun(
        "AgentControlWorktree.repository.remotes",
        canonical.projectWorkspace,
        ["remote"],
      ).pipe(
        Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)),
      )).stdout
        .split("\n")
        .map((value) => value.trim())
        .filter((value) => value.length > 0);
      const expectedKey = `github.com/${canonical.repository.nameWithOwner}`.toLowerCase();
      const orderedRemotes = [...remoteNames].toSorted((left, right) => {
        const rank = (name: string) => (name === "upstream" ? 0 : name === "origin" ? 1 : 2);
        return rank(left) - rank(right) || left.localeCompare(right);
      });
      let remote: { readonly name: string; readonly canonicalKey: string } | null = null;
      for (const name of orderedRemotes) {
        const result = yield* Effect.result(
          gitRun(
            "AgentControlWorktree.repository.remoteUrl",
            canonical.projectWorkspace,
            ["remote", "get-url", name],
            true,
          ),
        );
        if (result._tag === "Failure" || result.success.exitCode !== 0) continue;
        const canonicalKey = normalizeGitRemoteUrl(result.success.stdout.trim());
        if (canonicalKey === expectedKey) {
          remote = { name, canonicalKey };
          break;
        }
      }
      if (remote === null) {
        return yield* error("repository-unavailable", operation, projectId, taskId);
      }
      const defaultRefResult = yield* gitRun(
        "AgentControlWorktree.repository.defaultRemoteRef",
        canonical.projectWorkspace,
        ["symbolic-ref", "--quiet", `refs/remotes/${remote.name}/HEAD`],
        true,
      ).pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const prefix = `refs/remotes/${remote.name}/`;
      const symbolicRef = defaultRefResult.stdout.trim();
      if (defaultRefResult.exitCode !== 0 || !symbolicRef.startsWith(prefix)) {
        return yield* error("default-remote-ref-unavailable", operation, projectId, taskId);
      }
      const baseBranch = symbolicRef.slice(prefix.length);
      if (baseBranch.length === 0) {
        return yield* error("default-remote-ref-unavailable", operation, projectId, taskId);
      }
      const baseRef = `${remote.name}/${baseBranch}`;
      const resolved = yield* workflow
        .resolveRemoteTrackingCommit({
          cwd: canonical.projectWorkspace,
          refName: baseRef,
          fallbackRemoteName: remote.name,
        })
        .pipe(
          Effect.mapError(() =>
            error("default-remote-ref-unavailable", operation, projectId, taskId),
          ),
        );
      if (!GIT_OBJECT_ID.test(resolved.commitSha)) {
        return yield* error("default-remote-ref-unavailable", operation, projectId, taskId);
      }
      return {
        repositoryWorkspace: canonical.projectWorkspace,
        repositoryCommonDir,
        repository: {
          repositoryNodeId: canonical.repository.repositoryNodeId,
          nameWithOwner: canonical.repository.nameWithOwner,
          canonicalKey: remote.canonicalKey,
        },
        baseRef,
        baseCommitSha: resolved.commitSha,
      };
    },
  );

  const accepted = Effect.fn("AgentControlWorktreeController.accepted")(function* (
    command: AgentControlWorktreeCommand,
  ) {
    const outcome = yield* engine.dispatchController(command);
    return outcome._tag === "Accepted" ? outcome.result.state : yield* outcome.error;
  });

  const inspect = Effect.fn("AgentControlWorktreeController.inspect")(function* (
    state: AgentControlWorktreeReservationState,
  ): Effect.fn.Return<
    | { readonly _tag: "exact" }
    | { readonly _tag: "create-new" }
    | { readonly _tag: "create-existing" }
    | { readonly _tag: "attention"; readonly code: AgentControlWorktreeAttentionCode },
    AgentControlWorktreeRpcError
  > {
    yield* validateExistingAgentControlWorktreePath({
      target: state.internalWorktreePath,
      repositoryWorkspace: state.repositoryWorkspace,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.provideService(ServerConfig, serverConfig),
      Effect.mapError(() =>
        error(
          "worktree-path-invalid",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const worktreeResult = yield* gitRun(
      "AgentControlWorktree.inspect.worktrees",
      state.repositoryWorkspace,
      ["worktree", "list", "--porcelain"],
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const branchResult = yield* gitRun(
      "AgentControlWorktree.inspect.branch",
      state.repositoryWorkspace,
      ["rev-parse", "--verify", "--quiet", `refs/heads/${state.branchName}^{commit}`],
      true,
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    if (branchResult.exitCode !== 0 && branchResult.exitCode !== 1) {
      return { _tag: "attention", code: "git-state-ambiguous" };
    }
    const branchSha = branchResult.exitCode === 0 ? branchResult.stdout.trim() : null;
    if (branchSha !== null && branchSha !== state.baseCommitSha) {
      return { _tag: "attention", code: "branch-commit-mismatch" };
    }
    const entries = parseWorktrees(worktreeResult.stdout);
    const branchEntry = entries.find((entry) => entry.branch === state.branchName);
    const targetExists = yield* fs
      .exists(state.internalWorktreePath)
      .pipe(
        Effect.mapError(() =>
          error(
            "worktree-path-invalid",
            "reconcile",
            state.projectId,
            state.taskId,
            state.reservationId,
          ),
        ),
      );
    const targetEntry = entries.find(
      (entry) => path.resolve(entry.path) === state.internalWorktreePath,
    );
    if (branchEntry && path.resolve(branchEntry.path) !== state.internalWorktreePath) {
      return { _tag: "attention", code: "branch-in-other-worktree" };
    }
    if (targetExists && targetEntry === undefined) {
      return { _tag: "attention", code: "path-occupied" };
    }
    if (!targetExists && targetEntry !== undefined) {
      return { _tag: "attention", code: "worktree-registration-mismatch" };
    }
    if (!targetExists) {
      return branchSha === null ? { _tag: "create-new" } : { _tag: "create-existing" };
    }
    if (
      targetEntry?.detached ||
      targetEntry?.branch !== state.branchName ||
      targetEntry.head !== state.baseCommitSha
    ) {
      return {
        _tag: "attention",
        code:
          targetEntry?.head !== state.baseCommitSha
            ? "worktree-head-mismatch"
            : "worktree-branch-mismatch",
      };
    }
    const [head, branch, common, topLevel] = yield* Effect.all([
      gitRun("AgentControlWorktree.inspect.head", state.internalWorktreePath, [
        "rev-parse",
        "HEAD",
      ]),
      gitRun("AgentControlWorktree.inspect.symbolicBranch", state.internalWorktreePath, [
        "symbolic-ref",
        "--quiet",
        "--short",
        "HEAD",
      ]),
      gitRun("AgentControlWorktree.inspect.commonDir", state.internalWorktreePath, [
        "rev-parse",
        "--git-common-dir",
      ]),
      gitRun("AgentControlWorktree.inspect.topLevel", state.internalWorktreePath, [
        "rev-parse",
        "--show-toplevel",
      ]),
    ]).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const commonCandidate = path.isAbsolute(common.stdout.trim())
      ? common.stdout.trim()
      : path.resolve(state.internalWorktreePath, common.stdout.trim());
    const [actualCommon, actualTop] = yield* Effect.all([
      fs.realPath(commonCandidate),
      fs.realPath(topLevel.stdout.trim()),
    ]).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    if (
      actualCommon !== state.repositoryCommonDir ||
      actualTop !==
        (yield* fs
          .realPath(state.internalWorktreePath)
          .pipe(
            Effect.mapError(() =>
              error(
                "repository-unavailable",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              ),
            ),
          ))
    ) {
      return { _tag: "attention", code: "repository-identity-mismatch" };
    }
    if (branch.stdout.trim() !== state.branchName) {
      return { _tag: "attention", code: "worktree-branch-mismatch" };
    }
    if (head.stdout.trim() !== state.baseCommitSha) {
      return { _tag: "attention", code: "worktree-head-mismatch" };
    }
    return { _tag: "exact" };
  });

  const markAttention = (
    baseCommandId: CommandId,
    state: AgentControlWorktreeReservationState,
    code: AgentControlWorktreeAttentionCode,
  ) =>
    accepted({
      type: "agentControl.worktree.needsAttention",
      commandId: transitionCommandId(
        baseCommandId,
        state.reservationId,
        `attention:${code}`,
        state.revision,
      ),
      reservationId: state.reservationId,
      projectId: state.projectId,
      taskId: state.taskId,
      taskRevision: state.taskRevision,
      githubIntakeSequence: state.githubIntakeSequence,
      sourceIdentityFingerprint: state.sourceIdentityFingerprint,
      stageRunId: state.stageRunId,
      attemptId: state.attemptId,
      leaseId: state.leaseId,
      fenceToken: state.fenceToken,
      expectedRevision: state.revision,
      attentionCode: code,
    });

  const materialize = Effect.fn("AgentControlWorktreeController.materialize")(function* (
    baseCommandId: CommandId,
    initial: AgentControlWorktreeReservationState,
  ) {
    const lock = yield* getLock(initial.repositoryCommonDir);
    return yield* lock.withPermit(
      Effect.gen(function* () {
        let state = (yield* engine.loadAuthoritative(initial.reservationId)) ?? initial;
        if (state.status === "ready" || state.status === "needs-attention") return state;
        if (state.status === "reserved") {
          state = yield* accepted({
            type: "agentControl.worktree.materialization.start",
            commandId: transitionCommandId(
              baseCommandId,
              state.reservationId,
              "materializing",
              state.revision,
            ),
            reservationId: state.reservationId,
            projectId: state.projectId,
            taskId: state.taskId,
            taskRevision: state.taskRevision,
            githubIntakeSequence: state.githubIntakeSequence,
            sourceIdentityFingerprint: state.sourceIdentityFingerprint,
            stageRunId: state.stageRunId,
            attemptId: state.attemptId,
            leaseId: state.leaseId,
            fenceToken: state.fenceToken,
            expectedRevision: state.revision,
          });
        }
        let observation = yield* inspect(state).pipe(
          Effect.orElseSucceed(() => ({
            _tag: "attention" as const,
            code: "git-state-ambiguous" as const,
          })),
        );
        if (observation._tag === "attention") {
          return yield* markAttention(baseCommandId, state, observation.code);
        }
        if (observation._tag !== "exact") {
          const create = workflow.createWorktree({
            cwd: state.repositoryWorkspace,
            refName: observation._tag === "create-new" ? state.baseCommitSha : state.branchName,
            ...(observation._tag === "create-new"
              ? { newRefName: state.branchName, baseRefName: state.baseRef }
              : {}),
            path: state.internalWorktreePath,
          });
          const createResult = yield* Effect.result(create);
          observation = yield* inspect(state).pipe(
            Effect.orElseSucceed(() => ({
              _tag: "attention" as const,
              code: "git-state-ambiguous" as const,
            })),
          );
          if (observation._tag !== "exact") {
            const code =
              observation._tag === "attention"
                ? observation.code
                : createResult._tag === "Failure"
                  ? "git-state-ambiguous"
                  : "worktree-registration-mismatch";
            return yield* markAttention(baseCommandId, state, code);
          }
        }
        return yield* accepted({
          type: "agentControl.worktree.ready",
          commandId: transitionCommandId(
            baseCommandId,
            state.reservationId,
            "ready",
            state.revision,
          ),
          reservationId: state.reservationId,
          projectId: state.projectId,
          taskId: state.taskId,
          taskRevision: state.taskRevision,
          githubIntakeSequence: state.githubIntakeSequence,
          sourceIdentityFingerprint: state.sourceIdentityFingerprint,
          stageRunId: state.stageRunId,
          attemptId: state.attemptId,
          leaseId: state.leaseId,
          fenceToken: state.fenceToken,
          expectedRevision: state.revision,
          headCommitSha: state.baseCommitSha,
        });
      }),
    );
  });

  const reserveAndMaterialize: AgentControlWorktreeControllerShape["reserveAndMaterialize"] = (
    input,
  ) =>
    Effect.gen(function* () {
      const canonical = yield* preflight(input.projectId, input.taskId, "reserve");
      const existingProjection = yield* states
        .getByStage({
          projectId: input.projectId,
          taskId: input.taskId,
          stageRunId: canonical.stageRun.stageRunId,
          attemptId: canonical.stageRun.attemptId,
        })
        .pipe(
          Effect.mapError(() =>
            error("reservation-projection-corrupt", "reserve", input.projectId, input.taskId),
          ),
        );
      if (Option.isSome(existingProjection)) {
        const projected = existingProjection.value;
        if (
          projected.taskRevision !== canonical.task.revision ||
          projected.githubIntakeSequence !== canonical.task.githubIntakeSequence ||
          projected.sourceIdentityFingerprint !== canonical.sourceIdentityFingerprint ||
          projected.repository.repositoryNodeId !== canonical.repository.repositoryNodeId ||
          projected.repository.nameWithOwner !== canonical.repository.nameWithOwner
        ) {
          return yield* error(
            "source-snapshot-stale",
            "reserve",
            input.projectId,
            input.taskId,
            projected.reservationId,
          );
        }
        if (
          projected.leaseId !== canonical.lease.leaseId ||
          projected.fenceToken !== canonical.lease.fenceToken
        ) {
          return yield* error(
            "fence-token-mismatch",
            "reserve",
            input.projectId,
            input.taskId,
            projected.reservationId,
          );
        }
        const authoritative = yield* engine.loadAuthoritative(projected.reservationId);
        if (authoritative === null) {
          return yield* error(
            "reservation-projection-corrupt",
            "reserve",
            input.projectId,
            input.taskId,
            projected.reservationId,
          );
        }
        return yield* materialize(input.commandId, authoritative);
      }
      const repository = yield* resolveRepository(canonical, "reserve");
      const branchName = deriveAgentControlWorktreeBranchName({
        issueNumber: canonical.task.source.issueNumber,
        title: canonical.task.sourceSnapshot.title,
        taskId: canonical.task.taskId,
      });
      const validBranch = yield* gitRun(
        "AgentControlWorktree.branch.checkRefFormat",
        repository.repositoryWorkspace,
        ["check-ref-format", "--branch", branchName],
        true,
      ).pipe(
        Effect.mapError(() =>
          error("branch-name-invalid", "reserve", input.projectId, input.taskId),
        ),
      );
      if (validBranch.exitCode !== 0) {
        return yield* error("branch-name-invalid", "reserve", input.projectId, input.taskId);
      }
      const reservationId = yield* deriveAgentControlWorktreeReservationId({
        projectId: input.projectId,
        taskId: input.taskId,
        stageRunId: canonical.stageRun.stageRunId,
        attemptId: canonical.stageRun.attemptId,
        leaseId: canonical.lease.leaseId,
        fenceToken: canonical.lease.fenceToken,
        repositoryIdentity: {
          repositoryNodeId: repository.repository.repositoryNodeId,
          canonicalKey: repository.repository.canonicalKey,
        },
        baseCommitSha: repository.baseCommitSha,
      });
      const safePath = yield* deriveSafeAgentControlWorktreePath({
        projectId: input.projectId,
        reservationId,
        repositoryWorkspace: repository.repositoryWorkspace,
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.provideService(ServerConfig, serverConfig),
        Effect.mapError((cause) =>
          error(
            cause.reason === "target-exists" ? "reservation-conflict" : "worktree-path-invalid",
            "reserve",
            input.projectId,
            input.taskId,
            reservationId,
          ),
        ),
      );
      const state = yield* accepted({
        type: "agentControl.worktree.reserve",
        commandId: input.commandId,
        reservationId,
        projectId: input.projectId,
        taskId: input.taskId,
        taskRevision: canonical.task.revision,
        githubIntakeSequence: canonical.task.githubIntakeSequence,
        sourceIdentityFingerprint: canonical.sourceIdentityFingerprint,
        stageRunId: canonical.stageRun.stageRunId,
        attemptId: canonical.stageRun.attemptId,
        leaseId: canonical.lease.leaseId,
        fenceToken: canonical.lease.fenceToken,
        expectedRevision: 0,
        repository: repository.repository,
        repositoryWorkspace: repository.repositoryWorkspace,
        repositoryCommonDir: repository.repositoryCommonDir,
        baseRef: repository.baseRef,
        baseCommitSha: repository.baseCommitSha,
        branchName,
        internalWorktreePath: safePath.target,
      });
      return yield* materialize(input.commandId, state);
    });

  const reconcile: AgentControlWorktreeControllerShape["reconcile"] = (input) =>
    Effect.gen(function* () {
      const state = yield* engine.loadAuthoritative(input.reservationId);
      if (state === null || state.projectId !== input.projectId) {
        return yield* error(
          "reservation-missing",
          "reconcile",
          input.projectId,
          null,
          input.reservationId,
        );
      }
      yield* preflight(input.projectId, state.taskId, "reconcile");
      return yield* materialize(input.commandId, state);
    });

  return AgentControlWorktreeController.of({ reserveAndMaterialize, reconcile });
});

export const layer = Layer.effect(AgentControlWorktreeController, make);
