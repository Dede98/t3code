import {
  CommandId,
  type AgentControlTaskId,
  type AgentControlWorktreeAttentionCode,
  type AgentControlWorktreeCommand,
  type AgentControlWorktreeReservationState,
  AgentControlWorktreeRpcError,
  AgentControlWorktreeRejectedCommandCode,
  AgentControlWorktreeReservationState as AgentControlWorktreeReservationStateSchema,
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
import * as Schema from "effect/Schema";
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
  reserveAgentControlWorktreeTargetPath,
  releaseAgentControlWorktreeTargetPath,
  revalidateAgentControlWorktreePathIdentity,
  validateExistingAgentControlWorktreePath,
} from "../pathSafety.ts";
import { parseAgentControlWorktreeList } from "../gitState.ts";
import {
  expectedAgentControlWorktreeOwnershipMarker,
  fingerprintAgentControlWorktreeOwnership,
  ownershipMarkerPath,
  readAgentControlWorktreeOwnershipMarker,
  writeAgentControlWorktreeOwnershipMarker,
} from "../ownership.ts";
import { withAgentControlRepositoryLock } from "../repositoryLock.ts";
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

const encodeReservationState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeReservationStateSchema),
);
const decodeReservationState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeReservationStateSchema),
);
const isWorktreeRejectedCommandCode = Schema.is(AgentControlWorktreeRejectedCommandCode);
class AgentControlGitObservationIncomplete extends Schema.TaggedErrorClass<AgentControlGitObservationIncomplete>()(
  "AgentControlGitObservationIncomplete",
  {},
) {}

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
  const operationLocks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

  type CompositeType = "reserve-and-materialize" | "reconcile";
  type CompositeRow = {
    readonly commandId: string;
    readonly commandType: string;
    readonly inputFingerprint: string;
    readonly projectId: string;
    readonly taskId: string | null;
    readonly reservationId: string | null;
    readonly worktreeReservationId: string | null;
    readonly status: string;
    readonly resultJson: string | null;
    readonly rejectionCode: string | null;
  };
  const NON_TERMINAL_CONTROLLER_CODES = new Set<AgentControlWorktreeRpcError["code"]>([
    "internal-persistence-error",
    "repository-unavailable",
    "repository-lock-unavailable",
  ]);

  const compositeFingerprint = (input: {
    readonly commandId: CommandId;
    readonly commandType: CompositeType;
    readonly projectId: ProjectId;
    readonly taskId?: AgentControlTaskId;
    readonly reservationId?: AgentControlWorktreeReservationState["reservationId"];
  }) =>
    sha256FramedHex([
      "agent-control-worktree-controller-operation-v1",
      input.commandId,
      input.commandType,
      input.projectId,
      input.taskId ?? "",
      input.reservationId ?? "",
    ]);

  const readComposite = (commandId: CommandId) =>
    sql<CompositeRow>`
      SELECT command_id AS "commandId", command_type AS "commandType",
        input_fingerprint AS "inputFingerprint", project_id AS "projectId",
        task_id AS "taskId", reservation_id AS "reservationId",
        worktree_reservation_id AS "worktreeReservationId", status,
        result_json AS "resultJson", rejection_code AS "rejectionCode"
      FROM agent_control_worktree_controller_operations
      WHERE command_id = ${commandId}
    `;

  const beginComposite = Effect.fn("AgentControlWorktreeController.beginComposite")(function* (
    input: {
      readonly commandId: CommandId;
      readonly commandType: CompositeType;
      readonly projectId: ProjectId;
      readonly taskId?: AgentControlTaskId;
      readonly reservationId?: AgentControlWorktreeReservationState["reservationId"];
    },
    operation: AgentControlWorktreeRpcError["operation"],
  ) {
    const inputFingerprint = compositeFingerprint(input);
    const now = DateTime.formatIso(yield* DateTime.now);
    const row = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_worktree_controller_operations (
              command_id, command_type, input_fingerprint, project_id, task_id,
              reservation_id, worktree_reservation_id, status, result_json,
              rejection_code, created_at, updated_at, completed_at
            ) VALUES (
              ${input.commandId}, ${input.commandType}, ${inputFingerprint},
              ${input.projectId}, ${input.taskId ?? null}, ${input.reservationId ?? null},
              ${input.reservationId ?? null}, 'pending', NULL, NULL, ${now}, ${now}, NULL
            )
            ON CONFLICT(command_id) DO NOTHING
          `;
          return (yield* readComposite(input.commandId))[0]!;
        }),
      )
      .pipe(
        Effect.mapError(() =>
          error(
            "internal-persistence-error",
            operation,
            input.projectId,
            input.taskId ?? null,
            input.reservationId ?? null,
          ),
        ),
      );
    if (
      row.commandType !== input.commandType ||
      row.inputFingerprint !== inputFingerprint ||
      row.projectId !== input.projectId ||
      row.taskId !== (input.taskId ?? null) ||
      row.reservationId !== (input.reservationId ?? null)
    ) {
      return yield* error(
        "command-identity-mismatch",
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    if (row.status === "accepted") {
      if (row.resultJson === null) {
        return yield* error(
          "internal-persistence-error",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      const state = yield* decodeReservationState(row.resultJson).pipe(
        Effect.mapError(() =>
          error(
            "reservation-projection-corrupt",
            operation,
            input.projectId,
            input.taskId ?? null,
            input.reservationId ?? null,
          ),
        ),
      );
      if (
        state.projectId !== row.projectId ||
        (row.taskId !== null && state.taskId !== row.taskId) ||
        row.worktreeReservationId !== state.reservationId ||
        (row.reservationId !== null && row.reservationId !== state.reservationId)
      ) {
        return yield* error(
          "internal-persistence-error",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      return { _tag: "AcceptedReplay" as const, state };
    }
    if (row.status === "rejected") {
      if (!isWorktreeRejectedCommandCode(row.rejectionCode)) {
        return yield* error(
          "internal-persistence-error",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      return yield* error(
        row.rejectionCode,
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    if (row.status !== "pending") {
      return yield* error(
        "internal-persistence-error",
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    return { _tag: "Pending" as const, row };
  });

  const bindCompositeReservation = (
    commandId: CommandId,
    reservationId: AgentControlWorktreeReservationState["reservationId"],
    operation: AgentControlWorktreeRpcError["operation"],
    projectId: ProjectId,
    taskId: AgentControlTaskId | null,
  ) =>
    Effect.gen(function* () {
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
      UPDATE agent_control_worktree_controller_operations
      SET worktree_reservation_id = ${reservationId}, updated_at = ${now}
      WHERE command_id = ${commandId} AND status = 'pending'
        AND (worktree_reservation_id IS NULL OR worktree_reservation_id = ${reservationId})
      `.pipe(
        Effect.mapError(() =>
          error("internal-persistence-error", operation, projectId, taskId, reservationId),
        ),
      );
      const row = (yield* readComposite(commandId).pipe(
        Effect.mapError(() =>
          error("internal-persistence-error", operation, projectId, taskId, reservationId),
        ),
      ))[0];
      if (row?.worktreeReservationId !== reservationId) {
        return yield* error("reservation-conflict", operation, projectId, taskId, reservationId);
      }
    });

  const acceptComposite = Effect.fn("AgentControlWorktreeController.acceptComposite")(function* (
    commandId: CommandId,
    state: AgentControlWorktreeReservationState,
    operation: AgentControlWorktreeRpcError["operation"],
  ) {
    const resultJson = yield* encodeReservationState(state).pipe(
      Effect.mapError(() =>
        error(
          "internal-persistence-error",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const now = DateTime.formatIso(yield* DateTime.now);
    yield* sql`
      UPDATE agent_control_worktree_controller_operations
      SET status = 'accepted', result_json = ${resultJson}, rejection_code = NULL,
        worktree_reservation_id = ${state.reservationId}, updated_at = ${now},
        completed_at = ${now}
      WHERE command_id = ${commandId} AND status = 'pending'
    `.pipe(
      Effect.mapError(() =>
        error(
          "internal-persistence-error",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
  });

  const rejectComposite = Effect.fn("AgentControlWorktreeController.rejectComposite")(function* (
    commandId: CommandId,
    failure: AgentControlWorktreeRpcError,
  ) {
    if (NON_TERMINAL_CONTROLLER_CODES.has(failure.code)) return;
    const now = DateTime.formatIso(yield* DateTime.now);
    yield* sql`
      UPDATE agent_control_worktree_controller_operations
      SET status = 'rejected', rejection_code = ${failure.code}, updated_at = ${now},
        completed_at = ${now}
      WHERE command_id = ${commandId} AND status = 'pending'
    `.pipe(Effect.orDie);
  });

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

  const getOperationLock = (commandId: CommandId) =>
    SynchronizedRef.modifyEffect(operationLocks, (current) => {
      const existing = current.get(commandId);
      if (existing) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => {
          const next = new Map(current);
          next.set(commandId, lock);
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
            Effect.mapError((failure) =>
              error(
                failure._tag === "AgentControlPersistenceSqlError"
                  ? "internal-persistence-error"
                  : "stage-run-projection-corrupt",
                operation,
                projectId,
                taskId,
              ),
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
            Effect.mapError((failure) =>
              error(
                failure._tag === "AgentControlPersistenceSqlError"
                  ? "internal-persistence-error"
                  : "lease-projection-corrupt",
                operation,
                projectId,
                taskId,
              ),
            ),
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
    git
      .execute({
        operation,
        cwd,
        args,
        allowNonZeroExit,
        timeoutMs: 10_000,
        maxOutputBytes: 1_000_000,
      })
      .pipe(
        Effect.flatMap((result) =>
          result.stdoutTruncated || result.stderrTruncated
            ? Effect.fail(new AgentControlGitObservationIncomplete())
            : Effect.succeed(result),
        ),
      );

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
      const matchingRemotes: Array<{
        readonly name: string;
        readonly canonicalKey: string;
      }> = [];
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
          matchingRemotes.push({ name, canonicalKey });
        }
      }
      if (matchingRemotes.length !== 1) {
        return yield* error("repository-identity-mismatch", operation, projectId, taskId);
      }
      const remote = matchingRemotes[0]!;
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
      const commonInfo = yield* fs
        .stat(repositoryCommonDir)
        .pipe(Effect.mapError(() => error("repository-unavailable", operation, projectId, taskId)));
      const commonDirInode = Option.getOrUndefined(commonInfo.ino);
      if (commonInfo.type !== "Directory" || commonDirInode === undefined) {
        return yield* error("repository-identity-mismatch", operation, projectId, taskId);
      }
      return {
        repositoryWorkspace: canonical.projectWorkspace,
        repositoryCommonDir,
        repository: {
          repositoryNodeId: canonical.repository.repositoryNodeId,
          nameWithOwner: canonical.repository.nameWithOwner,
          canonicalKey: remote.canonicalKey,
          remoteName: remote.name,
          remoteUrl: remote.canonicalKey,
          defaultRemoteRef: symbolicRef,
          commonDirDevice: commonInfo.dev,
          commonDirInode,
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

  const inspectRepositoryIdentity = Effect.fn(
    "AgentControlWorktreeController.inspectRepositoryIdentity",
  )(function* (
    canonical: Effect.Success<ReturnType<typeof preflight>>,
    state: AgentControlWorktreeReservationState,
    operation: AgentControlWorktreeRpcError["operation"],
  ) {
    if (
      canonical.repository.repositoryNodeId !== state.repository.repositoryNodeId ||
      canonical.repository.nameWithOwner !== state.repository.nameWithOwner
    ) {
      return false;
    }
    const top = yield* gitRun(
      "AgentControlWorktree.repository.revalidateTopLevel",
      canonical.projectWorkspace,
      ["rev-parse", "--show-toplevel"],
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const common = yield* gitRun(
      "AgentControlWorktree.repository.revalidateCommonDir",
      canonical.projectWorkspace,
      ["rev-parse", "--git-common-dir"],
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const topCandidate = top.stdout.trim();
    const commonCandidate = path.isAbsolute(common.stdout.trim())
      ? common.stdout.trim()
      : path.resolve(canonical.projectWorkspace, common.stdout.trim());
    const [actualTop, actualProject, actualCommon, persistedWorkspace] = yield* Effect.all([
      fs.realPath(topCandidate),
      fs.realPath(canonical.projectWorkspace),
      fs.realPath(commonCandidate),
      fs.realPath(state.repositoryWorkspace),
    ]).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const relativeProject = path.relative(actualTop, actualProject);
    const commonInfo = yield* fs
      .stat(actualCommon)
      .pipe(
        Effect.mapError(() =>
          error(
            "repository-unavailable",
            operation,
            state.projectId,
            state.taskId,
            state.reservationId,
          ),
        ),
      );
    const commonInode = Option.getOrUndefined(commonInfo.ino);
    if (
      relativeProject === ".." ||
      relativeProject.startsWith(`..${path.sep}`) ||
      actualTop !== persistedWorkspace ||
      actualCommon !== state.repositoryCommonDir ||
      commonInfo.dev !== state.repository.commonDirDevice ||
      commonInode !== state.repository.commonDirInode
    ) {
      return false;
    }
    const remoteNames = (yield* gitRun(
      "AgentControlWorktree.repository.revalidateRemotes",
      state.repositoryWorkspace,
      ["remote"],
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    )).stdout
      .split("\n")
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    const matches: Array<string> = [];
    for (const remoteName of remoteNames) {
      const remote = yield* gitRun(
        "AgentControlWorktree.repository.revalidateRemoteUrl",
        state.repositoryWorkspace,
        ["remote", "get-url", remoteName],
        true,
      ).pipe(
        Effect.mapError(() =>
          error(
            "repository-unavailable",
            operation,
            state.projectId,
            state.taskId,
            state.reservationId,
          ),
        ),
      );
      if (
        remote.exitCode === 0 &&
        normalizeGitRemoteUrl(remote.stdout.trim()) === state.repository.canonicalKey
      ) {
        matches.push(remoteName);
      }
    }
    if (matches.length !== 1 || matches[0] !== state.repository.remoteName) return false;
    const remoteUrl = yield* gitRun(
      "AgentControlWorktree.repository.revalidateSelectedRemote",
      state.repositoryWorkspace,
      ["remote", "get-url", state.repository.remoteName],
      true,
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const defaultRef = yield* gitRun(
      "AgentControlWorktree.repository.revalidateDefaultRef",
      state.repositoryWorkspace,
      ["symbolic-ref", "--quiet", `refs/remotes/${state.repository.remoteName}/HEAD`],
      true,
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    const base = yield* gitRun(
      "AgentControlWorktree.repository.revalidateBase",
      state.repositoryWorkspace,
      ["rev-parse", "--verify", "--quiet", `${state.baseRef}^{commit}`],
      true,
    ).pipe(
      Effect.mapError(() =>
        error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    return (
      remoteUrl.exitCode === 0 &&
      normalizeGitRemoteUrl(remoteUrl.stdout.trim()) === state.repository.remoteUrl &&
      defaultRef.exitCode === 0 &&
      defaultRef.stdout.trim() === state.repository.defaultRemoteRef &&
      base.exitCode === 0 &&
      base.stdout.trim() === state.baseCommitSha
    );
  });

  const inspect = Effect.fn("AgentControlWorktreeController.inspect")(function* (
    state: AgentControlWorktreeReservationState,
    canonical: Effect.Success<ReturnType<typeof preflight>>,
    requireOwnership: boolean,
  ): Effect.fn.Return<
    | {
        readonly _tag: "exact";
        readonly ownershipFingerprint: string | null;
        readonly gitDir: string;
        readonly pathIdentity: Effect.Success<
          ReturnType<typeof validateExistingAgentControlWorktreePath>
        >;
      }
    | { readonly _tag: "create-new" }
    | { readonly _tag: "create-existing" }
    | { readonly _tag: "attention"; readonly code: AgentControlWorktreeAttentionCode },
    AgentControlWorktreeRpcError
  > {
    const pathIdentity = yield* validateExistingAgentControlWorktreePath({
      target: state.internalWorktreePath,
      repositoryWorkspace: state.repositoryWorkspace,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.provideService(ServerConfig, serverConfig),
      Effect.mapError((cause) =>
        error(
          cause.reason === "root-invalid" || cause.reason === "target-invalid"
            ? "internal-persistence-error"
            : "worktree-path-invalid",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    if (
      pathIdentity.rootIdentity.device !== state.worktreeRootDevice ||
      pathIdentity.rootIdentity.inode !== state.worktreeRootInode ||
      pathIdentity.parentIdentity.device !== state.worktreeParentDevice ||
      pathIdentity.parentIdentity.inode !== state.worktreeParentInode
    ) {
      return { _tag: "attention", code: "repository-identity-mismatch" };
    }
    if (!(yield* inspectRepositoryIdentity(canonical, state, "reconcile"))) {
      return { _tag: "attention", code: "repository-identity-mismatch" };
    }
    const worktreeResult = yield* gitRun(
      "AgentControlWorktree.inspect.worktrees",
      state.repositoryWorkspace,
      ["worktree", "list", "--porcelain", "-z"],
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
    const parsed = yield* Effect.try({
      try: () => parseAgentControlWorktreeList(worktreeResult.stdout),
      catch: () => null,
    }).pipe(Effect.orElseSucceed(() => null));
    if (parsed === null) {
      return { _tag: "attention", code: "worktree-registration-ambiguous" };
    }
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
      return yield* error(
        "repository-unavailable",
        "reconcile",
        state.projectId,
        state.taskId,
        state.reservationId,
      );
    }
    const branchSha = branchResult.exitCode === 0 ? branchResult.stdout.trim() : null;
    if (branchSha !== null && branchSha !== state.baseCommitSha) {
      return { _tag: "attention", code: "branch-commit-mismatch" };
    }
    const pathMatches = parsed.filter(
      (entry) => path.resolve(entry.path) === state.internalWorktreePath,
    );
    const branchMatches = parsed.filter((entry) => entry.branch === state.branchName);
    if (pathMatches.length > 1 || branchMatches.length > 1) {
      return { _tag: "attention", code: "worktree-registration-ambiguous" };
    }
    const targetExists =
      (yield* fs
        .exists(state.internalWorktreePath)
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
        )) || (yield* Effect.result(fs.readLink(state.internalWorktreePath)))._tag === "Success";
    const targetEntry = pathMatches[0];
    const branchEntry = branchMatches[0];
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
      targetEntry === undefined ||
      targetEntry.detached ||
      targetEntry.bare ||
      targetEntry.locked ||
      targetEntry.prunable ||
      targetEntry.branch !== state.branchName ||
      targetEntry.head !== state.baseCommitSha
    ) {
      return {
        _tag: "attention",
        code:
          targetEntry?.head !== state.baseCommitSha
            ? "worktree-head-mismatch"
            : targetEntry?.branch !== state.branchName || targetEntry.detached
              ? "worktree-branch-mismatch"
              : "worktree-registration-mismatch",
      };
    }
    const [head, branch, common, topLevel, gitDir, status] = yield* Effect.all([
      gitRun("AgentControlWorktree.inspect.head", state.internalWorktreePath, [
        "rev-parse",
        "HEAD",
      ]),
      gitRun(
        "AgentControlWorktree.inspect.symbolicBranch",
        state.internalWorktreePath,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        true,
      ),
      gitRun("AgentControlWorktree.inspect.commonDir", state.internalWorktreePath, [
        "rev-parse",
        "--git-common-dir",
      ]),
      gitRun("AgentControlWorktree.inspect.topLevel", state.internalWorktreePath, [
        "rev-parse",
        "--show-toplevel",
      ]),
      gitRun("AgentControlWorktree.inspect.gitDir", state.internalWorktreePath, [
        "rev-parse",
        "--git-dir",
      ]),
      gitRun("AgentControlWorktree.inspect.clean", state.internalWorktreePath, [
        "status",
        "--porcelain=v2",
        "-z",
        "--untracked-files=all",
        "--ignore-submodules=none",
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
    if (branch.exitCode !== 0 || branch.stdout.trim() !== state.branchName) {
      return { _tag: "attention", code: "worktree-branch-mismatch" };
    }
    if (head.stdout.trim() !== state.baseCommitSha) {
      return { _tag: "attention", code: "worktree-head-mismatch" };
    }
    const commonCandidate = path.isAbsolute(common.stdout.trim())
      ? common.stdout.trim()
      : path.resolve(state.internalWorktreePath, common.stdout.trim());
    const gitDirCandidate = path.isAbsolute(gitDir.stdout.trim())
      ? gitDir.stdout.trim()
      : path.resolve(state.internalWorktreePath, gitDir.stdout.trim());
    const [actualCommon, actualTop, actualTarget, actualGitDir] = yield* Effect.all([
      fs.realPath(commonCandidate),
      fs.realPath(topLevel.stdout.trim()),
      fs.realPath(state.internalWorktreePath),
      fs.realPath(gitDirCandidate),
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
      actualTop !== actualTarget ||
      actualTarget !== state.internalWorktreePath ||
      !path.relative(state.repositoryCommonDir, actualGitDir).startsWith(`worktrees${path.sep}`)
    ) {
      return { _tag: "attention", code: "repository-identity-mismatch" };
    }
    if (status.stdout.length !== 0) {
      return { _tag: "attention", code: "worktree-dirty" };
    }
    for (const gitPath of [
      "MERGE_HEAD",
      "rebase-merge",
      "rebase-apply",
      "CHERRY_PICK_HEAD",
      "REVERT_HEAD",
      "BISECT_LOG",
      "sequencer",
    ]) {
      const marker = yield* gitRun(
        "AgentControlWorktree.inspect.sequencerPath",
        state.internalWorktreePath,
        ["rev-parse", "--git-path", gitPath],
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
      const markerPath = path.isAbsolute(marker.stdout.trim())
        ? marker.stdout.trim()
        : path.resolve(state.internalWorktreePath, marker.stdout.trim());
      const present =
        (yield* fs
          .exists(markerPath)
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
          )) || (yield* Effect.result(fs.readLink(markerPath)))._tag === "Success";
      if (present) return { _tag: "attention", code: "worktree-sequencer-state" };
    }
    let ownershipFingerprint: string | null = null;
    if (requireOwnership) {
      const markerPath = yield* ownershipMarkerPath(state.internalWorktreePath, actualGitDir).pipe(
        Effect.provideService(Path.Path, path),
      );
      const markerLink = yield* Effect.result(fs.readLink(markerPath));
      if (markerLink._tag === "Success") {
        return { _tag: "attention", code: "ownership-mismatch" };
      }
      const markerExists = yield* fs
        .exists(markerPath)
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
        );
      if (!markerExists) return { _tag: "attention", code: "ownership-unproven" };
      const markerInfo = yield* fs
        .stat(markerPath)
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
        );
      const markerUid = Option.getOrUndefined(markerInfo.uid);
      if (
        markerInfo.type !== "File" ||
        (markerInfo.mode & 0o077) !== 0 ||
        (markerUid !== undefined &&
          typeof process.getuid === "function" &&
          markerUid !== process.getuid())
      ) {
        return { _tag: "attention", code: "ownership-mismatch" };
      }
      const marker = yield* readAgentControlWorktreeOwnershipMarker(markerPath).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.option,
      );
      if (Option.isNone(marker)) return { _tag: "attention", code: "ownership-mismatch" };
      const expected = expectedAgentControlWorktreeOwnershipMarker(state);
      ownershipFingerprint = fingerprintAgentControlWorktreeOwnership(marker.value);
      if (
        ownershipFingerprint !== fingerprintAgentControlWorktreeOwnership(expected) ||
        (state.ownershipFingerprint !== null && state.ownershipFingerprint !== ownershipFingerprint)
      ) {
        return { _tag: "attention", code: "ownership-mismatch" };
      }
    }
    yield* revalidateAgentControlWorktreePathIdentity(pathIdentity).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
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
    return { _tag: "exact", ownershipFingerprint, pathIdentity, gitDir: actualGitDir };
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
      withAgentControlRepositoryLock({
        repositoryCommonDir: initial.repositoryCommonDir,
        runtimeHolderId: holderId,
        effect: Effect.gen(function* () {
          let state = (yield* engine.loadAuthoritative(initial.reservationId)) ?? initial;
          if (state.status === "ready" || state.status === "needs-attention") return state;
          let canonical = yield* preflight(state.projectId, state.taskId, "reconcile");
          if (
            canonical.lease.leaseId !== state.leaseId ||
            canonical.lease.fenceToken !== state.fenceToken
          ) {
            return yield* error(
              "fence-token-mismatch",
              "reconcile",
              state.projectId,
              state.taskId,
              state.reservationId,
            );
          }
          if (
            canonical.task.revision !== state.taskRevision ||
            canonical.task.githubIntakeSequence !== state.githubIntakeSequence ||
            canonical.sourceIdentityFingerprint !== state.sourceIdentityFingerprint ||
            canonical.stageRun.stageRunId !== state.stageRunId ||
            canonical.stageRun.attemptId !== state.attemptId
          ) {
            return yield* error(
              "source-snapshot-stale",
              "reconcile",
              state.projectId,
              state.taskId,
              state.reservationId,
            );
          }
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
          let observation = yield* inspect(state, canonical, true);
          if (observation._tag === "attention") {
            return yield* markAttention(baseCommandId, state, observation.code);
          }
          if (observation._tag !== "exact") {
            const pathIdentity = yield* validateExistingAgentControlWorktreePath({
              target: state.internalWorktreePath,
              repositoryWorkspace: state.repositoryWorkspace,
            }).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
              Effect.provideService(ServerConfig, serverConfig),
              Effect.mapError((cause) =>
                error(
                  cause.reason === "root-invalid" || cause.reason === "target-invalid"
                    ? "internal-persistence-error"
                    : "worktree-path-invalid",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                ),
              ),
            );
            yield* revalidateAgentControlWorktreePathIdentity(pathIdentity).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
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
            if (!(yield* inspectRepositoryIdentity(canonical, state, "reconcile"))) {
              return yield* markAttention(baseCommandId, state, "repository-identity-mismatch");
            }
            const targetReservation = yield* Effect.result(
              reserveAgentControlWorktreeTargetPath(pathIdentity).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
              ),
            );
            if (targetReservation._tag === "Failure") {
              if (targetReservation.failure.reason === "target-exists") {
                return yield* markAttention(baseCommandId, state, "path-occupied");
              }
              return yield* error(
                targetReservation.failure.reason === "root-invalid" ||
                  targetReservation.failure.reason === "target-invalid"
                  ? "internal-persistence-error"
                  : "worktree-path-invalid",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            yield* Effect.addFinalizer(() =>
              releaseAgentControlWorktreeTargetPath(targetReservation.success).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.ignore,
              ),
            );
            const createResult = yield* Effect.result(
              workflow.createWorktree({
                cwd: state.repositoryWorkspace,
                refName: observation._tag === "create-new" ? state.baseCommitSha : state.branchName,
                ...(observation._tag === "create-new"
                  ? { newRefName: state.branchName, baseRefName: state.baseRef }
                  : {}),
                path: state.internalWorktreePath,
              }),
            );
            const afterCreate = yield* inspect(state, canonical, false);
            if (createResult._tag === "Failure") {
              if (afterCreate._tag === "attention" && afterCreate.code === "path-occupied") {
                yield* releaseAgentControlWorktreeTargetPath(targetReservation.success).pipe(
                  Effect.provideService(FileSystem.FileSystem, fs),
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
                return yield* error(
                  "repository-unavailable",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                );
              }
              if (afterCreate._tag === "attention") {
                return yield* markAttention(baseCommandId, state, afterCreate.code);
              }
              if (afterCreate._tag === "exact") {
                const owned = yield* inspect(state, canonical, true);
                return owned._tag === "attention"
                  ? yield* markAttention(baseCommandId, state, owned.code)
                  : yield* error(
                      "repository-unavailable",
                      "reconcile",
                      state.projectId,
                      state.taskId,
                      state.reservationId,
                    );
              }
              return yield* error(
                "repository-unavailable",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            if (afterCreate._tag !== "exact") {
              return yield* markAttention(
                baseCommandId,
                state,
                afterCreate._tag === "attention"
                  ? afterCreate.code
                  : "worktree-registration-mismatch",
              );
            }
            const materializedTargetInfo = yield* fs
              .stat(state.internalWorktreePath)
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
              );
            const materializedTargetInode = Option.getOrUndefined(materializedTargetInfo.ino);
            if (
              materializedTargetInfo.dev !== targetReservation.success.device ||
              materializedTargetInode !== targetReservation.success.inode
            ) {
              return yield* markAttention(baseCommandId, state, "repository-identity-mismatch");
            }
            yield* revalidateAgentControlWorktreePathIdentity(afterCreate.pathIdentity).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
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
            const markerPath = yield* ownershipMarkerPath(
              state.internalWorktreePath,
              afterCreate.gitDir,
            ).pipe(Effect.provideService(Path.Path, path));
            yield* Effect.scoped(
              writeAgentControlWorktreeOwnershipMarker(
                markerPath,
                expectedAgentControlWorktreeOwnershipMarker(state),
              ).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.provideService(Path.Path, path),
              ),
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
            observation = yield* inspect(state, canonical, true);
            if (observation._tag !== "exact") {
              return yield* markAttention(
                baseCommandId,
                state,
                observation._tag === "attention"
                  ? observation.code
                  : "worktree-registration-mismatch",
              );
            }
          }
          canonical = yield* preflight(state.projectId, state.taskId, "reconcile");
          const finalObservation = yield* inspect(state, canonical, true);
          if (finalObservation._tag !== "exact" || finalObservation.ownershipFingerprint === null) {
            return yield* markAttention(
              baseCommandId,
              state,
              finalObservation._tag === "attention"
                ? finalObservation.code
                : "worktree-registration-mismatch",
            );
          }
          const verifiedAt = DateTime.formatIso(yield* DateTime.now);
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
            ownershipFingerprint: finalObservation.ownershipFingerprint,
            verifiedAt,
          });
        }),
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.catchTag("AgentControlRepositoryLockError", () =>
          error(
            "repository-lock-unavailable",
            "reconcile",
            initial.projectId,
            initial.taskId,
            initial.reservationId,
          ),
        ),
      ),
    );
  });

  const reserveAndMaterialize: AgentControlWorktreeControllerShape["reserveAndMaterialize"] = (
    input,
  ) =>
    getOperationLock(input.commandId).pipe(
      Effect.flatMap((operationLock) =>
        operationLock.withPermit(
          Effect.gen(function* () {
            const composite = yield* beginComposite(
              {
                ...input,
                commandType: "reserve-and-materialize",
              },
              "reserve",
            );
            if (composite._tag === "AcceptedReplay") return composite.state;
            if (composite.row.worktreeReservationId !== null) {
              const mapped = yield* engine.loadAuthoritative(
                composite.row
                  .worktreeReservationId as AgentControlWorktreeReservationState["reservationId"],
              );
              if (
                mapped === null ||
                mapped.projectId !== input.projectId ||
                mapped.taskId !== input.taskId
              ) {
                return yield* error(
                  "reservation-projection-corrupt",
                  "reserve",
                  input.projectId,
                  input.taskId,
                );
              }
              const completed = yield* materialize(input.commandId, mapped).pipe(
                Effect.tapError((failure) => rejectComposite(input.commandId, failure)),
              );
              yield* acceptComposite(input.commandId, completed, "reserve");
              return completed;
            }
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
              yield* bindCompositeReservation(
                input.commandId,
                authoritative.reservationId,
                "reserve",
                input.projectId,
                input.taskId,
              );
              const completed = yield* materialize(input.commandId, authoritative).pipe(
                Effect.tapError((failure) => rejectComposite(input.commandId, failure)),
              );
              yield* acceptComposite(input.commandId, completed, "reserve");
              return completed;
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
                  cause.reason === "target-exists"
                    ? "reservation-conflict"
                    : cause.reason === "root-invalid" || cause.reason === "target-invalid"
                      ? "internal-persistence-error"
                      : "worktree-path-invalid",
                  "reserve",
                  input.projectId,
                  input.taskId,
                  reservationId,
                ),
              ),
            );
            const state = yield* accepted({
              type: "agentControl.worktree.reserve",
              commandId: transitionCommandId(input.commandId, reservationId, "reserve", 0),
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
              worktreeRootDevice: safePath.rootIdentity.device,
              worktreeRootInode: safePath.rootIdentity.inode,
              worktreeParentDevice: safePath.parentIdentity.device,
              worktreeParentInode: safePath.parentIdentity.inode,
            });
            yield* bindCompositeReservation(
              input.commandId,
              state.reservationId,
              "reserve",
              input.projectId,
              input.taskId,
            );
            const completed = yield* materialize(input.commandId, state).pipe(
              Effect.tapError((failure) => rejectComposite(input.commandId, failure)),
            );
            yield* acceptComposite(input.commandId, completed, "reserve");
            return completed;
          }).pipe(Effect.tapError((failure) => rejectComposite(input.commandId, failure))),
        ),
      ),
    );

  const reconcile: AgentControlWorktreeControllerShape["reconcile"] = (input) =>
    getOperationLock(input.commandId).pipe(
      Effect.flatMap((operationLock) =>
        operationLock.withPermit(
          Effect.gen(function* () {
            const composite = yield* beginComposite(
              {
                ...input,
                commandType: "reconcile",
              },
              "reconcile",
            );
            if (composite._tag === "AcceptedReplay") return composite.state;
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
            const completed = yield* materialize(input.commandId, state);
            yield* acceptComposite(input.commandId, completed, "reconcile");
            return completed;
          }).pipe(Effect.tapError((failure) => rejectComposite(input.commandId, failure))),
        ),
      ),
    );

  const useReadyWorktree: AgentControlWorktreeControllerShape["useReadyWorktree"] = (
    input,
    callback,
  ) =>
    Effect.gen(function* () {
      const state = yield* engine.loadAuthoritative(input.reservationId);
      if (
        state === null ||
        state.projectId !== input.projectId ||
        state.status !== "ready" ||
        state.verifiedAt === null ||
        state.ownershipFingerprint === null
      ) {
        return yield* error(
          state === null ? "reservation-missing" : "state-not-available",
          "materialize",
          input.projectId,
          state?.taskId ?? null,
          input.reservationId,
        );
      }
      const lock = yield* getLock(state.repositoryCommonDir);
      return yield* lock.withPermit(
        withAgentControlRepositoryLock({
          repositoryCommonDir: state.repositoryCommonDir,
          runtimeHolderId: holderId,
          effect: Effect.gen(function* () {
            const authoritative = yield* engine.loadAuthoritative(input.reservationId);
            if (
              authoritative === null ||
              authoritative.projectId !== input.projectId ||
              authoritative.status !== "ready"
            ) {
              return yield* error(
                "state-not-available",
                "materialize",
                input.projectId,
                state.taskId,
                input.reservationId,
              );
            }
            const canonical = yield* preflight(
              authoritative.projectId,
              authoritative.taskId,
              "materialize",
            );
            if (
              canonical.task.revision !== authoritative.taskRevision ||
              canonical.task.githubIntakeSequence !== authoritative.githubIntakeSequence ||
              canonical.sourceIdentityFingerprint !== authoritative.sourceIdentityFingerprint ||
              canonical.stageRun.stageRunId !== authoritative.stageRunId ||
              canonical.stageRun.attemptId !== authoritative.attemptId ||
              canonical.lease.leaseId !== authoritative.leaseId ||
              canonical.lease.fenceToken !== authoritative.fenceToken
            ) {
              return yield* error(
                "source-snapshot-stale",
                "materialize",
                input.projectId,
                authoritative.taskId,
                input.reservationId,
              );
            }
            const observation = yield* inspect(authoritative, canonical, true);
            if (
              observation._tag !== "exact" ||
              observation.ownershipFingerprint !== authoritative.ownershipFingerprint
            ) {
              return yield* error(
                observation._tag === "attention" &&
                  observation.code === "repository-identity-mismatch"
                  ? "repository-identity-mismatch"
                  : "state-not-available",
                "materialize",
                input.projectId,
                authoritative.taskId,
                input.reservationId,
              );
            }
            return yield* Effect.scoped(callback(authoritative));
          }),
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.catchTag("AgentControlRepositoryLockError", () =>
            error(
              "repository-lock-unavailable",
              "materialize",
              input.projectId,
              state.taskId,
              input.reservationId,
            ),
          ),
        ),
      );
    });

  return AgentControlWorktreeController.of({
    reserveAndMaterialize,
    reconcile,
    useReadyWorktree,
  });
});

export const layer = Layer.effect(AgentControlWorktreeController, make);
