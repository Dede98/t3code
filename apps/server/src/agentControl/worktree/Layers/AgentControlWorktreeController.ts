// @effect-diagnostics nodeBuiltinImport:off - no-follow pathname probes have no Effect FileSystem equivalent.
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
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
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
  foldAuthoritativeWorktreeReservationStream,
  sameAgentControlWorktreeReservationState,
} from "../authoritative.ts";
import {
  acquireAgentControlWorktreeTargetPath,
  deriveSafeAgentControlWorktreePath,
  type AgentControlPathIdentity,
  type AgentControlWorktreeTargetIdentity,
  releaseAgentControlWorktreeTargetPath,
  revalidateAgentControlWorktreePathIdentity,
  validateExistingAgentControlWorktreePath,
  verifyAgentControlWorktreeTargetPath,
} from "../pathSafety.ts";
import { parseAgentControlPorcelainV2Status, parseAgentControlWorktreeList } from "../gitState.ts";
import {
  expectedAgentControlWorktreeOwnershipMarker,
  fingerprintAgentControlWorktreeOwnership,
  inspectAgentControlWorktreeOwnershipMarker,
  ownershipMarkerPath,
  writeAgentControlWorktreeOwnershipMarker,
} from "../ownership.ts";
import { withAgentControlRepositoryLock } from "../repositoryLock.ts";
import {
  AgentControlWorktreeController,
  type AgentControlWorktreeControllerShape,
} from "../Services/AgentControlWorktreeController.ts";
import {
  AgentControlWorktreeControllerHooks,
  type AgentControlWorktreeLifecycleCheckpoint,
} from "../Services/AgentControlWorktreeControllerHooks.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeStateRepository } from "../Services/AgentControlWorktreeStateRepository.ts";

const GIT_OBJECT_ID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const INTERNAL_TRANSITION_COMMAND_PREFIX = "agent-control-internal-worktree-v1-";

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
    `${INTERNAL_TRANSITION_COMMAND_PREFIX}${sha256FramedHex([
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

const nodeErrno = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "code" in cause
    ? (cause as { readonly code?: unknown }).code
    : undefined;

const pathExistsNoFollow = (target: string) =>
  Effect.tryPromise({
    try: () => NodeFSP.lstat(target).then(() => true),
    catch: (cause) =>
      nodeErrno(cause) === "ENOENT" ? null : new AgentControlGitObservationIncomplete(),
  }).pipe(
    Effect.catch((failure) => (failure === null ? Effect.succeed(false) : Effect.fail(failure))),
  );

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
  const worktreeEvents = yield* AgentControlWorktreeEventStore;
  const controllerHooks = yield* AgentControlWorktreeControllerHooks;
  const states = yield* AgentControlWorktreeStateRepository;
  const holderId = yield* leaseEngine.runtimeHolderId;
  const locks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
  const operationLocks = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
  const lifecycleCheckpoint = (
    checkpoint: AgentControlWorktreeLifecycleCheckpoint,
    commandId: CommandId,
    reservationId: AgentControlWorktreeReservationState["reservationId"] | null,
  ) =>
    controllerHooks.afterLifecycleCheckpoint?.(checkpoint, commandId, reservationId) ?? Effect.void;

  type CompositeType = "reserve-and-materialize" | "reconcile";
  type CompositeRow = {
    readonly commandId: string;
    readonly commandType: string;
    readonly inputFingerprint: string;
    readonly projectId: string;
    readonly taskId: string | null;
    readonly reservationId: string | null;
    readonly worktreeReservationId: string | null;
    readonly targetGenerationId: string | null;
    readonly status: string;
    readonly pendingToken: string | null;
    readonly claimRuntimeId: string | null;
    readonly claimAttemptId: string | null;
    readonly materializationPhase: string;
    readonly gitCreatedDevice: number | null;
    readonly gitCreatedInode: number | null;
    readonly gitCreatedGitDir: string | null;
    readonly markedOwnershipFingerprint: string | null;
    readonly resultJson: string | null;
    readonly resultStatus: string | null;
    readonly resultReservationId: string | null;
    readonly resultRevision: number | null;
    readonly resultSequence: number | null;
    readonly rejectionCode: string | null;
    readonly revision: number;
  };
  type CompositeClaim = {
    readonly commandId: CommandId;
    readonly commandType: CompositeType;
    readonly inputFingerprint: string;
    readonly pendingToken: string;
    readonly revision: number;
    readonly row: CompositeRow;
  };
  const recoveryClaims = yield* SynchronizedRef.make(new Map<CommandId, CompositeClaim>());
  const NON_TERMINAL_CONTROLLER_CODES = new Set<AgentControlWorktreeRpcError["code"]>([
    "internal-persistence-error",
    "repository-unavailable",
    "repository-lock-unavailable",
  ]);

  const rememberRecoveryClaim = (claim: CompositeClaim) =>
    SynchronizedRef.update(recoveryClaims, (current) => {
      const next = new Map(current);
      next.set(claim.commandId, claim);
      return next;
    });

  const forgetRecoveryClaim = (commandId: CommandId) =>
    SynchronizedRef.update(recoveryClaims, (current) => {
      if (!current.has(commandId)) return current;
      const next = new Map(current);
      next.delete(commandId);
      return next;
    });

  const releaseCompositeClaim = Effect.fn("AgentControlWorktreeController.releaseCompositeClaim")(
    function* (claim: CompositeClaim): Effect.fn.Return<void, "sql" | "cas"> {
      const now = DateTime.formatIso(yield* DateTime.now);
      const released = yield* sql<{ readonly commandId: string }>`
      UPDATE agent_control_worktree_controller_operations
      SET pending_token = NULL, claim_runtime_id = NULL, claim_attempt_id = NULL,
        claim_started_at = NULL, updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND command_type = ${claim.commandType}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${claim.pendingToken}
        AND revision = ${claim.revision}
        AND status = 'pending'
      RETURNING command_id AS "commandId"
    `.pipe(Effect.mapError(() => "sql" as const));
      if (released.length !== 1 || released[0]?.commandId !== claim.commandId) {
        return yield* Effect.fail("cas" as const);
      }
    },
  );

  const recoverRememberedClaim = Effect.fn("AgentControlWorktreeController.recoverRememberedClaim")(
    function* (
      commandId: CommandId,
      operation: AgentControlWorktreeRpcError["operation"],
      projectId: ProjectId,
      taskId: AgentControlTaskId | null,
      reservationId: AgentControlWorktreeReservationState["reservationId"] | null,
    ) {
      const remembered = (yield* SynchronizedRef.get(recoveryClaims)).get(commandId);
      if (remembered === undefined) return;
      const released = yield* Effect.result(releaseCompositeClaim(remembered));
      if (released._tag === "Success") {
        yield* forgetRecoveryClaim(commandId);
        return;
      }
      if (released.failure === "cas") {
        // The remembered generation is no longer ours. Forgetting it is not a
        // successful release; the authoritative begin path below still decides
        // whether the row is terminal, unclaimed, or owned by another holder.
        yield* forgetRecoveryClaim(commandId);
        return;
      }
      return yield* error(
        "internal-persistence-error",
        operation,
        projectId,
        taskId,
        reservationId,
      );
    },
  );

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
        worktree_reservation_id AS "worktreeReservationId",
        target_generation_id AS "targetGenerationId", status,
        pending_token AS "pendingToken", claim_runtime_id AS "claimRuntimeId",
        claim_attempt_id AS "claimAttemptId",
        materialization_phase AS "materializationPhase",
        git_created_device AS "gitCreatedDevice",
        git_created_inode AS "gitCreatedInode",
        git_created_git_dir AS "gitCreatedGitDir",
        marked_ownership_fingerprint AS "markedOwnershipFingerprint",
        result_json AS "resultJson", result_status AS "resultStatus",
        result_reservation_id AS "resultReservationId",
        result_revision AS "resultRevision", result_sequence AS "resultSequence",
        rejection_code AS "rejectionCode", revision
      FROM agent_control_worktree_controller_operations
      WHERE command_id = ${commandId}
    `;

  const failAfterCompositeCasLoss = Effect.fn(
    "AgentControlWorktreeController.failAfterCompositeCasLoss",
  )(function* (
    claim: CompositeClaim,
    operation: AgentControlWorktreeRpcError["operation"],
    projectId: ProjectId,
    taskId: AgentControlTaskId | null,
    reservationId: AgentControlWorktreeReservationState["reservationId"] | null,
  ) {
    const row = (yield* readComposite(claim.commandId).pipe(
      Effect.mapError(() =>
        error("internal-persistence-error", operation, projectId, taskId, reservationId),
      ),
    ))[0];
    if (row === undefined) {
      return yield* error(
        "internal-persistence-error",
        operation,
        projectId,
        taskId,
        reservationId,
      );
    }
    if (
      row.commandType !== claim.commandType ||
      row.inputFingerprint !== claim.inputFingerprint ||
      row.commandId !== claim.commandId
    ) {
      return yield* error("command-identity-mismatch", operation, projectId, taskId, reservationId);
    }
    return yield* error(
      row.status === "rejected" ? "command-previously-rejected" : "lease-recovery-required",
      operation,
      projectId,
      taskId,
      reservationId,
    );
  });

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
    if (input.commandId.startsWith(INTERNAL_TRANSITION_COMMAND_PREFIX)) {
      return yield* error(
        "validation",
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    const inputFingerprint = compositeFingerprint(input);
    const now = DateTime.formatIso(yield* DateTime.now);
    const pendingToken = NodeCrypto.randomUUID();
    const claimAttemptId = NodeCrypto.randomUUID();
    const targetGenerationId = NodeCrypto.randomBytes(32).toString("hex");
    const row = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_worktree_controller_operations (
              command_id, command_type, input_fingerprint, project_id, task_id,
              reservation_id, worktree_reservation_id, status, result_json,
              rejection_code, pending_token, claim_runtime_id, claim_attempt_id,
              claim_started_at, materialization_phase, created_at, updated_at,
              completed_at, revision, target_generation_id
            ) VALUES (
              ${input.commandId}, ${input.commandType}, ${inputFingerprint},
              ${input.projectId}, ${input.taskId ?? null}, ${input.reservationId ?? null},
              NULL, 'pending', NULL, NULL, ${pendingToken},
              ${holderId}, ${claimAttemptId}, ${now},
              'unbound',
              ${now}, ${now}, NULL, 1, ${
                input.reservationId === undefined ? targetGenerationId : null
              }
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
      if (
        row.resultJson === null ||
        row.resultReservationId === null ||
        row.resultRevision === null ||
        row.resultSequence === null
      ) {
        return yield* error(
          "reservation-projection-corrupt",
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
        row.resultStatus !== state.status ||
        (row.resultStatus !== "ready" && row.resultStatus !== "needs-attention") ||
        row.resultReservationId !== state.reservationId ||
        row.resultRevision !== state.revision ||
        row.resultSequence !== state.sequence
      ) {
        return yield* error(
          "reservation-projection-corrupt",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      if (
        state.projectId !== row.projectId ||
        (row.taskId !== null && state.taskId !== row.taskId) ||
        row.worktreeReservationId !== state.reservationId ||
        (row.reservationId !== null && row.reservationId !== state.reservationId) ||
        row.targetGenerationId !== state.targetGenerationId
      ) {
        return yield* error(
          "reservation-projection-corrupt",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      const folded = yield* foldAuthoritativeWorktreeReservationStream(
        state.reservationId,
        worktreeEvents,
      ).pipe(
        Effect.mapError((failure) =>
          error(
            failure._tag === "AgentControlPersistenceSqlError"
              ? "internal-persistence-error"
              : "reservation-projection-corrupt",
            operation,
            input.projectId,
            input.taskId ?? null,
            input.reservationId ?? null,
          ),
        ),
      );
      if (
        Option.isNone(folded) ||
        row.resultRevision > folded.value.statesByVersion.length ||
        folded.value.events[row.resultRevision - 1]?.sequence !== row.resultSequence ||
        !sameAgentControlWorktreeReservationState(
          folded.value.statesByVersion[row.resultRevision - 1]!,
          state,
        ) ||
        !sameAgentControlWorktreeReservationState(folded.value.state, state)
      ) {
        return yield* error(
          "reservation-projection-corrupt",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      const authoritative = yield* engine.loadAuthoritative(state.reservationId);
      if (
        authoritative === null ||
        !sameAgentControlWorktreeReservationState(authoritative, state)
      ) {
        return yield* error(
          "reservation-projection-corrupt",
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
    if (row.pendingToken === pendingToken) {
      return {
        _tag: "Pending" as const,
        claim: {
          commandId: input.commandId,
          commandType: input.commandType,
          inputFingerprint,
          pendingToken,
          revision: row.revision,
          row,
        } satisfies CompositeClaim,
      };
    }
    if (row.pendingToken !== null || row.claimRuntimeId !== null || row.claimAttemptId !== null) {
      return yield* error(
        "lease-recovery-required",
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    const claimed = yield* sql<CompositeRow>`
      UPDATE agent_control_worktree_controller_operations
      SET pending_token = ${pendingToken}, claim_runtime_id = ${holderId},
        claim_attempt_id = ${claimAttemptId}, claim_started_at = ${now},
        updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${input.commandId}
        AND command_type = ${input.commandType}
        AND input_fingerprint = ${inputFingerprint}
        AND status = 'pending' AND pending_token IS NULL
        AND claim_runtime_id IS NULL AND claim_attempt_id IS NULL
        AND revision = ${row.revision}
      RETURNING command_id AS "commandId", command_type AS "commandType",
        input_fingerprint AS "inputFingerprint", project_id AS "projectId",
        task_id AS "taskId", reservation_id AS "reservationId",
        worktree_reservation_id AS "worktreeReservationId",
        target_generation_id AS "targetGenerationId", status,
        pending_token AS "pendingToken", claim_runtime_id AS "claimRuntimeId",
        claim_attempt_id AS "claimAttemptId",
        materialization_phase AS "materializationPhase",
        git_created_device AS "gitCreatedDevice",
        git_created_inode AS "gitCreatedInode",
        git_created_git_dir AS "gitCreatedGitDir",
        marked_ownership_fingerprint AS "markedOwnershipFingerprint",
        result_json AS "resultJson", result_status AS "resultStatus",
        result_reservation_id AS "resultReservationId",
        result_revision AS "resultRevision", result_sequence AS "resultSequence",
        rejection_code AS "rejectionCode", revision
    `.pipe(
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
    const claimedRow = claimed[0];
    if (claimed.length !== 1 || claimedRow?.pendingToken !== pendingToken) {
      const current = (yield* readComposite(input.commandId).pipe(
        Effect.mapError(() =>
          error(
            "internal-persistence-error",
            operation,
            input.projectId,
            input.taskId ?? null,
            input.reservationId ?? null,
          ),
        ),
      ))[0];
      if (
        current === undefined ||
        current.commandType !== input.commandType ||
        current.inputFingerprint !== inputFingerprint
      ) {
        return yield* error(
          current === undefined ? "internal-persistence-error" : "command-identity-mismatch",
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
      }
      return yield* error(
        "lease-recovery-required",
        operation,
        input.projectId,
        input.taskId ?? null,
        input.reservationId ?? null,
      );
    }
    return {
      _tag: "Pending" as const,
      claim: {
        commandId: input.commandId,
        commandType: input.commandType,
        inputFingerprint,
        pendingToken,
        revision: claimedRow.revision,
        row: claimedRow,
      } satisfies CompositeClaim,
    };
  });

  const bindCompositeReservation = (
    claim: CompositeClaim,
    reservationId: AgentControlWorktreeReservationState["reservationId"],
    targetGenerationId: string,
    operation: AgentControlWorktreeRpcError["operation"],
    projectId: ProjectId,
    taskId: AgentControlTaskId | null,
  ) =>
    Effect.gen(function* () {
      const now = DateTime.formatIso(yield* DateTime.now);
      const updated = yield* sql<CompositeRow>`
      UPDATE agent_control_worktree_controller_operations
      SET worktree_reservation_id = ${reservationId},
        target_generation_id = ${targetGenerationId}, materialization_phase = 'reserved',
        updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND command_type = ${claim.commandType}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${claim.pendingToken}
        AND revision = ${claim.revision} AND status = 'pending'
        AND (worktree_reservation_id IS NULL OR worktree_reservation_id = ${reservationId})
        AND (target_generation_id IS NULL OR target_generation_id = ${targetGenerationId})
        AND materialization_phase IN ('unbound', 'reserved')
      RETURNING command_id AS "commandId", command_type AS "commandType",
        input_fingerprint AS "inputFingerprint", project_id AS "projectId",
        task_id AS "taskId", reservation_id AS "reservationId",
        worktree_reservation_id AS "worktreeReservationId",
        target_generation_id AS "targetGenerationId", status,
        pending_token AS "pendingToken", claim_runtime_id AS "claimRuntimeId",
        claim_attempt_id AS "claimAttemptId",
        materialization_phase AS "materializationPhase",
        git_created_device AS "gitCreatedDevice",
        git_created_inode AS "gitCreatedInode",
        git_created_git_dir AS "gitCreatedGitDir",
        marked_ownership_fingerprint AS "markedOwnershipFingerprint",
        result_json AS "resultJson", result_status AS "resultStatus",
        result_reservation_id AS "resultReservationId",
        result_revision AS "resultRevision", result_sequence AS "resultSequence",
        rejection_code AS "rejectionCode", revision
      `.pipe(
        Effect.mapError(() =>
          error("internal-persistence-error", operation, projectId, taskId, reservationId),
        ),
      );
      const row = updated[0];
      if (updated.length !== 1 || row?.worktreeReservationId !== reservationId) {
        return yield* failAfterCompositeCasLoss(claim, operation, projectId, taskId, reservationId);
      }
      return { ...claim, revision: row.revision, row } satisfies CompositeClaim;
    });

  const acceptComposite = Effect.fn("AgentControlWorktreeController.acceptComposite")(function* (
    claim: CompositeClaim,
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
    const updated = yield* sql<{ readonly commandId: string }>`
      UPDATE agent_control_worktree_controller_operations
      SET status = 'accepted', result_json = ${resultJson}, result_status = ${state.status},
        rejection_code = NULL,
        worktree_reservation_id = ${state.reservationId}, updated_at = ${now},
        completed_at = ${now}, result_reservation_id = ${state.reservationId},
        result_revision = ${state.revision}, result_sequence = ${state.sequence},
        pending_token = NULL, claim_runtime_id = NULL, claim_attempt_id = NULL,
        claim_started_at = NULL, materialization_phase = 'terminal',
        revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND command_type = ${claim.commandType}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${claim.pendingToken}
        AND revision = ${claim.revision} AND status = 'pending'
      RETURNING command_id AS "commandId"
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
    if (updated.length !== 1) {
      return yield* failAfterCompositeCasLoss(
        claim,
        operation,
        state.projectId,
        state.taskId,
        state.reservationId,
      );
    }
  });

  const rejectComposite = Effect.fn("AgentControlWorktreeController.rejectComposite")(function* (
    claim: CompositeClaim,
    failure: AgentControlWorktreeRpcError,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const terminal = !NON_TERMINAL_CONTROLLER_CODES.has(failure.code);
    const update = terminal
      ? sql<{ readonly commandId: string }>`
      UPDATE agent_control_worktree_controller_operations
      SET status = 'rejected', rejection_code = ${failure.code}, updated_at = ${now},
        completed_at = ${now}, pending_token = NULL, claim_runtime_id = NULL,
        claim_attempt_id = NULL, claim_started_at = NULL,
        materialization_phase = 'terminal', revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND command_type = ${claim.commandType}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${claim.pendingToken}
        AND revision = ${claim.revision} AND status = 'pending'
      RETURNING command_id AS "commandId"
    `
      : sql<{ readonly commandId: string }>`
      UPDATE agent_control_worktree_controller_operations
      SET pending_token = NULL, claim_runtime_id = NULL, claim_attempt_id = NULL,
        claim_started_at = NULL, updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND command_type = ${claim.commandType}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${claim.pendingToken}
        AND revision = ${claim.revision} AND status = 'pending'
      RETURNING command_id AS "commandId"
    `;
    const updated = yield* update.pipe(
      Effect.mapError(() =>
        error(
          "internal-persistence-error",
          failure.operation,
          failure.projectId,
          failure.taskId,
          failure.reservationId,
        ),
      ),
    );
    if (updated.length !== 1) {
      return yield* failAfterCompositeCasLoss(
        claim,
        failure.operation,
        failure.projectId,
        failure.taskId,
        failure.reservationId,
      );
    }
  });

  const advanceOwnedClaim = <R>(
    owner: Ref.Ref<CompositeClaim | null>,
    effect: Effect.Effect<CompositeClaim, AgentControlWorktreeRpcError, R>,
  ) => Effect.uninterruptible(effect.pipe(Effect.tap((claim) => Ref.set(owner, claim))));

  const cleanupOwnedClaim = Effect.fn("AgentControlWorktreeController.cleanupOwnedClaim")(
    function* (
      owner: Ref.Ref<CompositeClaim | null>,
      operation: AgentControlWorktreeRpcError["operation"],
      projectId: ProjectId,
      taskId: AgentControlTaskId | null,
      reservationId: AgentControlWorktreeReservationState["reservationId"] | null,
    ) {
      const claim = yield* Ref.get(owner);
      if (claim === null) return;
      const released = yield* Effect.result(releaseCompositeClaim(claim));
      if (released._tag === "Success") {
        yield* Ref.set(owner, null);
        yield* forgetRecoveryClaim(claim.commandId);
        return;
      }
      if (released.failure === "sql") {
        yield* rememberRecoveryClaim(claim);
      } else {
        yield* forgetRecoveryClaim(claim.commandId);
      }
      return yield* error(
        released.failure === "sql" ? "internal-persistence-error" : "lease-recovery-required",
        operation,
        projectId,
        taskId,
        reservationId,
      );
    },
  );

  const runCompositeLifecycle = <R>(
    input: {
      readonly commandId: CommandId;
      readonly commandType: CompositeType;
      readonly projectId: ProjectId;
      readonly taskId?: AgentControlTaskId;
      readonly reservationId?: AgentControlWorktreeReservationState["reservationId"];
    },
    operation: AgentControlWorktreeRpcError["operation"],
    use: (
      claim: CompositeClaim,
      owner: Ref.Ref<CompositeClaim | null>,
    ) => Effect.Effect<
      {
        readonly state: AgentControlWorktreeReservationState;
        readonly claim: CompositeClaim;
      },
      AgentControlWorktreeRpcError,
      R
    >,
  ): Effect.Effect<AgentControlWorktreeReservationState, AgentControlWorktreeRpcError, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* recoverRememberedClaim(
          input.commandId,
          operation,
          input.projectId,
          input.taskId ?? null,
          input.reservationId ?? null,
        );
        const owner = yield* Ref.make<CompositeClaim | null>(null);
        const composite = yield* beginComposite(input, operation);
        if (composite._tag === "AcceptedReplay") return composite.state;
        yield* Ref.set(owner, composite.claim);

        const operationExit = yield* Effect.exit(
          restore(
            use(composite.claim, owner).pipe(
              Effect.tap(
                () =>
                  controllerHooks.beforeCompositeAccept?.(composite.claim.commandId) ?? Effect.void,
              ),
            ),
          ),
        );
        if (Exit.isSuccess(operationExit)) {
          const acceptedExit = yield* Effect.exit(
            acceptComposite(operationExit.value.claim, operationExit.value.state, operation),
          );
          if (Exit.isSuccess(acceptedExit)) {
            yield* Ref.set(owner, null);
            yield* forgetRecoveryClaim(input.commandId);
            return operationExit.value.state;
          }
          const cleanupExit = yield* Effect.exit(
            cleanupOwnedClaim(
              owner,
              operation,
              input.projectId,
              input.taskId ?? operationExit.value.state.taskId,
              input.reservationId ?? operationExit.value.state.reservationId,
            ),
          );
          return yield* Effect.failCause(
            Exit.isFailure(cleanupExit)
              ? Cause.combine(acceptedExit.cause, cleanupExit.cause)
              : acceptedExit.cause,
          );
        }

        let cause = operationExit.cause;
        const failure = Cause.findErrorOption(cause);
        if (
          !Cause.hasInterrupts(cause) &&
          !Cause.hasDies(cause) &&
          Option.isSome(failure) &&
          failure.value._tag === "AgentControlWorktreeRpcError" &&
          !NON_TERMINAL_CONTROLLER_CODES.has(failure.value.code)
        ) {
          const claim = yield* Ref.get(owner);
          if (claim !== null) {
            const rejectedExit = yield* Effect.exit(rejectComposite(claim, failure.value));
            if (Exit.isSuccess(rejectedExit)) {
              yield* Ref.set(owner, null);
              yield* forgetRecoveryClaim(input.commandId);
              return yield* Effect.failCause(cause);
            }
            cause = Cause.combine(cause, rejectedExit.cause);
          }
        }

        const cleanupExit = yield* Effect.exit(
          cleanupOwnedClaim(
            owner,
            operation,
            input.projectId,
            input.taskId ?? null,
            input.reservationId ?? null,
          ),
        );
        return yield* Effect.failCause(
          Exit.isFailure(cleanupExit) ? Cause.combine(cause, cleanupExit.cause) : cause,
        );
      }),
    );

  const transitionCompositePhase = Effect.fn(
    "AgentControlWorktreeController.transitionCompositePhase",
  )(function* (input: {
    readonly claim: CompositeClaim;
    readonly from: string;
    readonly to: "materializing" | "git-created" | "ownership-marked";
    readonly state: AgentControlWorktreeReservationState;
    readonly gitCreatedDevice?: number;
    readonly gitCreatedInode?: number;
    readonly gitCreatedGitDir?: string;
    readonly ownershipFingerprint?: string;
  }) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const updated = yield* sql<CompositeRow>`
      UPDATE agent_control_worktree_controller_operations
      SET materialization_phase = ${input.to},
        git_created_device = ${
          input.to === "git-created" ? input.gitCreatedDevice! : input.claim.row.gitCreatedDevice
        },
        git_created_inode = ${
          input.to === "git-created" ? input.gitCreatedInode! : input.claim.row.gitCreatedInode
        },
        git_created_git_dir = ${
          input.to === "git-created" ? input.gitCreatedGitDir! : input.claim.row.gitCreatedGitDir
        },
        marked_ownership_fingerprint = ${
          input.to === "ownership-marked" ? input.ownershipFingerprint! : null
        },
        updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${input.claim.commandId}
        AND command_type = ${input.claim.commandType}
        AND input_fingerprint = ${input.claim.inputFingerprint}
        AND status = 'pending' AND pending_token = ${input.claim.pendingToken}
        AND revision = ${input.claim.revision}
        AND materialization_phase = ${input.from}
        AND worktree_reservation_id = ${input.state.reservationId}
      RETURNING command_id AS "commandId", command_type AS "commandType",
        input_fingerprint AS "inputFingerprint", project_id AS "projectId",
        task_id AS "taskId", reservation_id AS "reservationId",
        worktree_reservation_id AS "worktreeReservationId",
        target_generation_id AS "targetGenerationId", status,
        pending_token AS "pendingToken", claim_runtime_id AS "claimRuntimeId",
        claim_attempt_id AS "claimAttemptId",
        materialization_phase AS "materializationPhase",
        git_created_device AS "gitCreatedDevice",
        git_created_inode AS "gitCreatedInode",
        git_created_git_dir AS "gitCreatedGitDir",
        marked_ownership_fingerprint AS "markedOwnershipFingerprint",
        result_json AS "resultJson", result_status AS "resultStatus",
        result_reservation_id AS "resultReservationId",
        result_revision AS "resultRevision", result_sequence AS "resultSequence",
        rejection_code AS "rejectionCode", revision
    `.pipe(
      Effect.mapError(() =>
        error(
          "internal-persistence-error",
          "reconcile",
          input.state.projectId,
          input.state.taskId,
          input.state.reservationId,
        ),
      ),
    );
    const row = updated[0];
    if (updated.length !== 1 || row === undefined) {
      return yield* failAfterCompositeCasLoss(
        input.claim,
        "reconcile",
        input.state.projectId,
        input.state.taskId,
        input.state.reservationId,
      );
    }
    return {
      ...input.claim,
      revision: row.revision,
      row,
    } satisfies CompositeClaim;
  });

  type TargetClaimRow = {
    readonly commandId: string;
    readonly inputFingerprint: string;
    readonly pendingToken: string;
    readonly claimAttemptId: string;
    readonly targetGeneration: string;
    readonly reservationId: string;
    readonly targetPath: string;
    readonly parentPath: string;
    readonly parentDevice: number;
    readonly parentInode: number;
    readonly targetDevice: number | null;
    readonly targetInode: number | null;
    readonly targetUid: number | null;
    readonly targetMode: number | null;
    readonly phase: string;
    readonly closedGitDevice: number | null;
    readonly closedGitInode: number | null;
    readonly closedGitDir: string | null;
    readonly closedOwnershipFingerprint: string | null;
    readonly revision: number;
  };
  type TargetResource = {
    readonly row: TargetClaimRow;
    readonly identity: AgentControlWorktreeTargetIdentity;
    readonly cleanupActive: Ref.Ref<boolean> | null;
    readonly empty: boolean;
  };
  const targetClaimColumns = sql`
    command_id AS "commandId", input_fingerprint AS "inputFingerprint",
    pending_token AS "pendingToken", claim_attempt_id AS "claimAttemptId",
    target_generation AS "targetGeneration", reservation_id AS "reservationId",
    target_path AS "targetPath", parent_path AS "parentPath",
    parent_device AS "parentDevice", parent_inode AS "parentInode",
    target_device AS "targetDevice", target_inode AS "targetInode",
    target_uid AS "targetUid", target_mode AS "targetMode", phase,
    closed_git_device AS "closedGitDevice", closed_git_inode AS "closedGitInode",
    closed_git_dir AS "closedGitDir",
    closed_ownership_fingerprint AS "closedOwnershipFingerprint", revision
  `;
  const targetClaimError = (
    state: AgentControlWorktreeReservationState,
    code: AgentControlWorktreeRpcError["code"] = "internal-persistence-error",
  ) => error(code, "reconcile", state.projectId, state.taskId, state.reservationId);
  const toTargetIdentity = (row: TargetClaimRow): AgentControlWorktreeTargetIdentity | null =>
    row.phase === "acquired" &&
    row.targetDevice !== null &&
    row.targetInode !== null &&
    row.targetUid !== null &&
    row.targetMode !== null
      ? {
          path: row.targetPath,
          device: row.targetDevice,
          inode: row.targetInode,
          uid: row.targetUid,
          mode: row.targetMode,
          parentIdentity: {
            path: row.parentPath,
            device: row.parentDevice,
            inode: row.parentInode,
          },
        }
      : null;

  const closeTargetClaim = Effect.fn("AgentControlWorktreeController.closeTargetClaim")(function* (
    claim: CompositeClaim,
    row: TargetClaimRow,
    state: AgentControlWorktreeReservationState,
    phase: "released" | "materialized" | "retained-attention",
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const closed = yield* sql<TargetClaimRow>`
        UPDATE agent_control_worktree_target_claims
        SET pending_token = ${claim.pendingToken},
          claim_attempt_id = ${claim.row.claimAttemptId},
          phase = ${phase},
          closed_git_device = ${phase === "released" ? null : claim.row.gitCreatedDevice},
          closed_git_inode = ${phase === "released" ? null : claim.row.gitCreatedInode},
          closed_git_dir = ${phase === "released" ? null : claim.row.gitCreatedGitDir},
          closed_ownership_fingerprint = ${
            phase === "released" ? null : claim.row.markedOwnershipFingerprint
          },
          updated_at = ${now}, revision = revision + 1
        WHERE command_id = ${claim.commandId}
          AND input_fingerprint = ${claim.inputFingerprint}
          AND pending_token = ${row.pendingToken}
          AND claim_attempt_id = ${row.claimAttemptId}
          AND target_generation = ${row.targetGeneration}
          AND reservation_id = ${state.reservationId}
          AND target_path = ${row.targetPath}
          AND revision = ${row.revision}
          AND phase IN ('prepared', 'acquired')
          AND EXISTS (
            SELECT 1
            FROM agent_control_worktree_controller_operations AS operation
            WHERE operation.command_id = ${claim.commandId}
              AND operation.command_type = ${claim.commandType}
              AND operation.input_fingerprint = ${claim.inputFingerprint}
              AND operation.pending_token = ${claim.pendingToken}
              AND operation.claim_attempt_id = ${claim.row.claimAttemptId}
              AND operation.revision = ${claim.revision}
              AND operation.status = 'pending'
              AND operation.worktree_reservation_id = ${state.reservationId}
              AND operation.target_generation_id = ${state.targetGenerationId}
          )
        RETURNING ${targetClaimColumns}
      `.pipe(Effect.mapError(() => targetClaimError(state)));
    if (closed.length !== 1 || closed[0]?.commandId !== claim.commandId) {
      return yield* targetClaimError(state, "lease-recovery-required");
    }
    return closed[0]!;
  });

  const cleanupTargetClaim = Effect.fn("AgentControlWorktreeController.cleanupTargetClaim")(
    function* (
      claim: CompositeClaim,
      row: TargetClaimRow,
      identity: AgentControlWorktreeTargetIdentity,
      state: AgentControlWorktreeReservationState,
    ) {
      const current = yield* sql<TargetClaimRow>`
        SELECT ${targetClaimColumns}
        FROM agent_control_worktree_target_claims
        WHERE command_id = ${claim.commandId}
          AND input_fingerprint = ${claim.inputFingerprint}
          AND target_generation = ${row.targetGeneration}
          AND reservation_id = ${state.reservationId}
          AND target_path = ${identity.path}
          AND parent_path = ${identity.parentIdentity.path}
          AND parent_device = ${identity.parentIdentity.device}
          AND parent_inode = ${identity.parentIdentity.inode}
      `.pipe(Effect.mapError(() => targetClaimError(state)));
      const authoritative = current[0];
      if (
        current.length !== 1 ||
        authoritative === undefined ||
        (authoritative.phase === "acquired" &&
          (authoritative.targetDevice !== identity.device ||
            authoritative.targetInode !== identity.inode ||
            authoritative.targetUid !== identity.uid ||
            authoritative.targetMode !== identity.mode))
      ) {
        return yield* targetClaimError(state, "lease-recovery-required");
      }
      yield* lifecycleCheckpoint("before-target-cleanup", claim.commandId, state.reservationId);
      yield* releaseAgentControlWorktreeTargetPath(identity).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.mapError(() => targetClaimError(state, "repository-unavailable")),
      );
      yield* lifecycleCheckpoint(
        "after-target-remove-before-claim-delete",
        claim.commandId,
        state.reservationId,
      );
      yield* closeTargetClaim(claim, authoritative, state, "released");
      yield* lifecycleCheckpoint("after-target-cleanup", claim.commandId, state.reservationId);
    },
  );

  const typedFailuresAsDefects = (cause: Cause.Cause<unknown>): Cause.Cause<never> =>
    Cause.fromReasons(
      cause.reasons.map(
        (reason): Cause.Reason<never> =>
          Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason,
      ),
    );

  const targetFinalizer = (
    active: Ref.Ref<boolean>,
    claim: CompositeClaim,
    row: TargetClaimRow,
    identity: AgentControlWorktreeTargetIdentity,
    state: AgentControlWorktreeReservationState,
    operationExit: Exit.Exit<unknown, unknown>,
  ) =>
    Ref.get(active).pipe(
      Effect.flatMap((shouldCleanup) =>
        shouldCleanup ? cleanupTargetClaim(claim, row, identity, state) : Effect.void,
      ),
      Effect.exit,
      Effect.flatMap((cleanupExit) => {
        if (Exit.isSuccess(cleanupExit)) return Effect.void;
        return Effect.failCause(
          Exit.isFailure(operationExit)
            ? Cause.combine(
                typedFailuresAsDefects(operationExit.cause),
                typedFailuresAsDefects(cleanupExit.cause),
              )
            : typedFailuresAsDefects(cleanupExit.cause),
        );
      }),
    );

  const prepareTargetClaim = Effect.fn("AgentControlWorktreeController.prepareTargetClaim")(
    function* (
      claim: CompositeClaim,
      state: AgentControlWorktreeReservationState,
      pathIdentity: {
        readonly target: string;
        readonly parentIdentity: AgentControlPathIdentity;
      },
    ) {
      if (claim.row.claimAttemptId === null) {
        return yield* targetClaimError(state, "lease-recovery-required");
      }
      if (claim.row.targetGenerationId !== state.targetGenerationId) {
        return yield* targetClaimError(state, "lease-recovery-required");
      }
      const now = DateTime.formatIso(yield* DateTime.now);
      const rows = yield* sql<TargetClaimRow>`
        INSERT INTO agent_control_worktree_target_claims (
          command_id, input_fingerprint, pending_token, claim_attempt_id,
          target_generation, reservation_id, target_path, parent_path,
          parent_device, parent_inode, phase, created_at, updated_at
        )
        SELECT ${claim.commandId}, ${claim.inputFingerprint}, ${claim.pendingToken},
          ${claim.row.claimAttemptId}, ${state.targetGenerationId}, ${state.reservationId},
          ${pathIdentity.target}, ${pathIdentity.parentIdentity.path},
          ${pathIdentity.parentIdentity.device}, ${pathIdentity.parentIdentity.inode},
          'prepared', ${now}, ${now}
        WHERE EXISTS (
          SELECT 1
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${claim.commandId}
            AND command_type = ${claim.commandType}
            AND input_fingerprint = ${claim.inputFingerprint}
            AND pending_token = ${claim.pendingToken}
            AND claim_attempt_id = ${claim.row.claimAttemptId}
            AND revision = ${claim.revision}
            AND status = 'pending'
            AND worktree_reservation_id = ${state.reservationId}
        )
        ON CONFLICT(command_id) DO UPDATE SET
          pending_token = excluded.pending_token,
          claim_attempt_id = excluded.claim_attempt_id,
          phase = 'prepared',
          target_device = NULL,
          target_inode = NULL,
          target_uid = NULL,
          target_mode = NULL,
          updated_at = excluded.updated_at,
          revision = agent_control_worktree_target_claims.revision + 1
        WHERE agent_control_worktree_target_claims.input_fingerprint
              = excluded.input_fingerprint
          AND agent_control_worktree_target_claims.target_generation
              = excluded.target_generation
          AND agent_control_worktree_target_claims.reservation_id
              = excluded.reservation_id
          AND agent_control_worktree_target_claims.target_path = excluded.target_path
          AND agent_control_worktree_target_claims.parent_path = excluded.parent_path
          AND agent_control_worktree_target_claims.parent_device = excluded.parent_device
          AND agent_control_worktree_target_claims.parent_inode = excluded.parent_inode
          AND agent_control_worktree_target_claims.phase = 'released'
        RETURNING ${targetClaimColumns}
      `.pipe(Effect.mapError(() => targetClaimError(state)));
      const row = rows[0];
      if (rows.length !== 1 || row === undefined) {
        return yield* targetClaimError(state, "lease-recovery-required");
      }
      return row;
    },
  );

  const persistAcquiredTargetClaim = Effect.fn(
    "AgentControlWorktreeController.persistAcquiredTargetClaim",
  )(function* (
    claim: CompositeClaim,
    row: TargetClaimRow,
    identity: AgentControlWorktreeTargetIdentity,
    state: AgentControlWorktreeReservationState,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    const updated = yield* sql<TargetClaimRow>`
      UPDATE agent_control_worktree_target_claims
      SET target_device = ${identity.device}, target_inode = ${identity.inode},
        target_uid = ${identity.uid}, target_mode = ${identity.mode},
        pending_token = ${claim.pendingToken},
        claim_attempt_id = ${claim.row.claimAttemptId},
        phase = 'acquired', updated_at = ${now}, revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${row.pendingToken}
        AND claim_attempt_id = ${row.claimAttemptId}
        AND target_generation = ${row.targetGeneration}
        AND reservation_id = ${state.reservationId}
        AND target_path = ${identity.path}
        AND parent_path = ${identity.parentIdentity.path}
        AND parent_device = ${identity.parentIdentity.device}
        AND parent_inode = ${identity.parentIdentity.inode}
        AND revision = ${row.revision}
        AND phase = 'prepared'
      RETURNING ${targetClaimColumns}
    `.pipe(Effect.mapError(() => targetClaimError(state)));
    const acquired = updated[0];
    if (updated.length !== 1 || acquired === undefined) {
      return yield* targetClaimError(state, "lease-recovery-required");
    }
    return acquired;
  });

  const acquireTargetResource = Effect.fn("AgentControlWorktreeController.acquireTargetResource")(
    function* (
      claim: CompositeClaim,
      state: AgentControlWorktreeReservationState,
      pathIdentity: {
        readonly target: string;
        readonly rootIdentity: AgentControlPathIdentity;
        readonly parentIdentity: AgentControlPathIdentity;
      },
      recoveredPrepared?: TargetClaimRow,
    ) {
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const prepared =
            recoveredPrepared ?? (yield* prepareTargetClaim(claim, state, pathIdentity));
          const cleanupActive = yield* Ref.make(true);
          const cleanupIdentity = yield* Ref.make<AgentControlWorktreeTargetIdentity | null>(null);
          yield* Effect.addFinalizer((operationExit) =>
            Ref.get(cleanupIdentity).pipe(
              Effect.flatMap((identity) =>
                identity === null
                  ? Effect.void
                  : targetFinalizer(cleanupActive, claim, prepared, identity, state, operationExit),
              ),
            ),
          );
          const acquireExit = yield* Effect.exit(
            acquireAgentControlWorktreeTargetPath({
              ...pathIdentity,
              fault: controllerHooks.targetPathFault,
            }).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.mapError((cause) =>
                targetClaimError(
                  state,
                  cause.reason === "target-exists"
                    ? "reservation-conflict"
                    : cause.reason === "observation-failed"
                      ? "repository-unavailable"
                      : "worktree-path-invalid",
                ),
              ),
            ),
          );
          if (Exit.isFailure(acquireExit)) {
            let cause = acquireExit.cause;
            const failure = Cause.findErrorOption(cause);
            if (Option.isSome(failure) && failure.value.code === "reservation-conflict") {
              const deleteExit = yield* Effect.exit(
                closeTargetClaim(claim, prepared, state, "released"),
              );
              if (Exit.isFailure(deleteExit)) cause = Cause.combine(cause, deleteExit.cause);
            }
            return yield* Effect.failCause(cause);
          }
          const identity = acquireExit.value;
          yield* Ref.set(cleanupIdentity, identity);
          const acquired = yield* persistAcquiredTargetClaim(claim, prepared, identity, state);
          yield* restore(
            Effect.gen(function* () {
              yield* lifecycleCheckpoint(
                "after-target-acquired",
                claim.commandId,
                state.reservationId,
              );
              yield* verifyAgentControlWorktreeTargetPath(pathIdentity, identity).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.mapError((cause) =>
                  targetClaimError(
                    state,
                    cause.reason === "observation-failed"
                      ? "repository-unavailable"
                      : "worktree-path-invalid",
                  ),
                ),
              );
            }),
          );
          return {
            row: acquired,
            identity,
            cleanupActive,
            empty: true,
          } satisfies TargetResource;
        }),
      );
    },
  );

  const recoverTargetResource = Effect.fn("AgentControlWorktreeController.recoverTargetResource")(
    function* (
      claim: CompositeClaim,
      state: AgentControlWorktreeReservationState,
      pathIdentity: {
        readonly target: string;
        readonly rootIdentity: AgentControlPathIdentity;
        readonly parentIdentity: AgentControlPathIdentity;
      },
    ): Effect.fn.Return<TargetResource | null, AgentControlWorktreeRpcError, Scope.Scope> {
      const rows = yield* sql<TargetClaimRow>`
      SELECT ${targetClaimColumns}
      FROM agent_control_worktree_target_claims
      WHERE target_path = ${pathIdentity.target}
        AND phase IN ('prepared', 'acquired')
    `.pipe(Effect.mapError(() => targetClaimError(state)));
      const row = rows[0];
      if (row === undefined) return null;
      if (
        rows.length !== 1 ||
        row.commandId !== claim.commandId ||
        row.inputFingerprint !== claim.inputFingerprint ||
        row.targetGeneration !== state.targetGenerationId ||
        claim.row.targetGenerationId !== state.targetGenerationId ||
        row.reservationId !== state.reservationId ||
        row.parentPath !== pathIdentity.parentIdentity.path ||
        row.parentDevice !== pathIdentity.parentIdentity.device ||
        row.parentInode !== pathIdentity.parentIdentity.inode ||
        claim.row.claimAttemptId === null
      ) {
        return yield* targetClaimError(state, "reservation-conflict");
      }
      const observed = yield* Effect.tryPromise({
        try: async () => {
          try {
            return Option.some(await NodeFSP.lstat(row.targetPath));
          } catch (cause) {
            if (nodeErrno(cause) === "ENOENT") return Option.none();
            throw cause;
          }
        },
        catch: () => targetClaimError(state, "repository-unavailable"),
      });
      if (Option.isNone(observed)) {
        if (row.phase !== "prepared" && row.phase !== "acquired") {
          return yield* targetClaimError(state, "repository-unavailable");
        }
        const now = DateTime.formatIso(yield* DateTime.now);
        const rebound = yield* sql<TargetClaimRow>`
          UPDATE agent_control_worktree_target_claims
          SET pending_token = ${claim.pendingToken},
            claim_attempt_id = ${claim.row.claimAttemptId},
            phase = 'prepared',
            target_device = NULL, target_inode = NULL,
            target_uid = NULL, target_mode = NULL,
            updated_at = ${now}, revision = revision + 1
          WHERE command_id = ${claim.commandId}
            AND input_fingerprint = ${claim.inputFingerprint}
            AND pending_token = ${row.pendingToken}
            AND claim_attempt_id = ${row.claimAttemptId}
            AND target_generation = ${row.targetGeneration}
            AND revision = ${row.revision}
            AND phase = ${row.phase}
            AND EXISTS (
              SELECT 1
              FROM agent_control_worktree_controller_operations AS operation
              WHERE operation.command_id = ${claim.commandId}
                AND operation.command_type = ${claim.commandType}
                AND operation.input_fingerprint = ${claim.inputFingerprint}
                AND operation.pending_token = ${claim.pendingToken}
                AND operation.claim_attempt_id = ${claim.row.claimAttemptId}
                AND operation.revision = ${claim.revision}
                AND operation.status = 'pending'
                AND operation.worktree_reservation_id = ${state.reservationId}
                AND operation.target_generation_id = ${state.targetGenerationId}
            )
          RETURNING ${targetClaimColumns}
        `.pipe(Effect.mapError(() => targetClaimError(state)));
        if (rebound.length !== 1 || rebound[0] === undefined) {
          return yield* targetClaimError(state, "lease-recovery-required");
        }
        return yield* acquireTargetResource(claim, state, pathIdentity, rebound[0]);
      }
      const info = observed.value;
      if (row.phase === "prepared") {
        if (
          !info.isDirectory() ||
          info.isSymbolicLink() ||
          (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
          (info.mode & 0o777) !== 0o700 ||
          info.ino < 0
        ) {
          return yield* targetClaimError(state, "reservation-conflict");
        }
        const children = yield* fs
          .readDirectory(row.targetPath)
          .pipe(Effect.mapError(() => targetClaimError(state, "repository-unavailable")));
        if (children.length !== 0) {
          return yield* targetClaimError(state, "reservation-conflict");
        }
        const identity = {
          path: row.targetPath,
          device: info.dev,
          inode: info.ino,
          uid: info.uid,
          mode: info.mode,
          parentIdentity: {
            path: row.parentPath,
            device: row.parentDevice,
            inode: row.parentInode,
          },
        } satisfies AgentControlWorktreeTargetIdentity;
        const acquired = yield* persistAcquiredTargetClaim(claim, row, identity, state);
        const cleanupActive = yield* Ref.make(true);
        yield* Effect.addFinalizer((operationExit) =>
          targetFinalizer(cleanupActive, claim, acquired, identity, state, operationExit),
        );
        yield* verifyAgentControlWorktreeTargetPath(pathIdentity, identity).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.mapError(() => targetClaimError(state, "repository-unavailable")),
        );
        return { row: acquired, identity, cleanupActive, empty: true };
      }
      const identity = toTargetIdentity(row);
      if (
        identity === null ||
        !info.isDirectory() ||
        info.isSymbolicLink() ||
        info.dev !== identity.device ||
        info.ino !== identity.inode ||
        info.uid !== identity.uid ||
        info.mode !== identity.mode
      ) {
        return yield* targetClaimError(state, "reservation-conflict");
      }
      const children = yield* fs
        .readDirectory(identity.path)
        .pipe(Effect.mapError(() => targetClaimError(state, "repository-unavailable")));
      const now = DateTime.formatIso(yield* DateTime.now);
      const rebound = yield* sql<TargetClaimRow>`
      UPDATE agent_control_worktree_target_claims
      SET pending_token = ${claim.pendingToken},
        claim_attempt_id = ${claim.row.claimAttemptId}, updated_at = ${now},
        revision = revision + 1
      WHERE command_id = ${claim.commandId}
        AND input_fingerprint = ${claim.inputFingerprint}
        AND pending_token = ${row.pendingToken}
        AND claim_attempt_id = ${row.claimAttemptId}
        AND target_generation = ${row.targetGeneration}
        AND revision = ${row.revision}
        AND phase = 'acquired'
        AND EXISTS (
          SELECT 1
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${claim.commandId}
            AND command_type = ${claim.commandType}
            AND input_fingerprint = ${claim.inputFingerprint}
            AND pending_token = ${claim.pendingToken}
            AND claim_attempt_id = ${claim.row.claimAttemptId}
            AND revision = ${claim.revision}
            AND status = 'pending'
            AND worktree_reservation_id = ${state.reservationId}
        )
      RETURNING ${targetClaimColumns}
    `.pipe(Effect.mapError(() => targetClaimError(state)));
      const reboundRow = rebound[0];
      if (rebound.length !== 1 || reboundRow === undefined) {
        return yield* targetClaimError(state, "lease-recovery-required");
      }
      if (children.length !== 0) {
        return { row: reboundRow, identity, cleanupActive: null, empty: false };
      }
      const cleanupActive = yield* Ref.make(true);
      yield* Effect.addFinalizer((operationExit) =>
        targetFinalizer(cleanupActive, claim, reboundRow, identity, state, operationExit),
      );
      yield* verifyAgentControlWorktreeTargetPath(pathIdentity, identity).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.mapError(() => targetClaimError(state, "repository-unavailable")),
      );
      return { row: reboundRow, identity, cleanupActive, empty: true };
    },
  );

  const completeTargetResource = Effect.fn("AgentControlWorktreeController.completeTargetResource")(
    function* (
      claim: CompositeClaim,
      resource: TargetResource,
      state: AgentControlWorktreeReservationState,
      phase: "materialized" | "retained-attention" = "materialized",
    ) {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          yield* closeTargetClaim(claim, resource.row, state, phase);
          if (resource.cleanupActive !== null) {
            yield* Ref.set(resource.cleanupActive, false);
          }
        }),
      );
    },
  );

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

  const authorityFingerprint = (canonical: Effect.Success<ReturnType<typeof preflight>>) =>
    sha256FramedHex([
      "agent-control-worktree-authority-preflight-v1",
      canonical.projectWorkspace,
      JSON.stringify(canonical.repository),
      JSON.stringify(canonical.task),
      canonical.sourceIdentityFingerprint,
      JSON.stringify(canonical.stageRun),
      JSON.stringify(canonical.lease),
    ]);

  const ensureCanonicalBinding = Effect.fn("AgentControlWorktreeController.ensureCanonicalBinding")(
    function* (
      canonical: Effect.Success<ReturnType<typeof preflight>>,
      state: AgentControlWorktreeReservationState,
      operation: AgentControlWorktreeRpcError["operation"],
    ) {
      if (
        canonical.task.revision !== state.taskRevision ||
        canonical.task.githubIntakeSequence !== state.githubIntakeSequence ||
        canonical.sourceIdentityFingerprint !== state.sourceIdentityFingerprint ||
        canonical.stageRun.stageRunId !== state.stageRunId ||
        canonical.stageRun.attemptId !== state.attemptId ||
        canonical.lease.leaseId !== state.leaseId ||
        canonical.lease.fenceToken !== state.fenceToken
      ) {
        return yield* error(
          "source-snapshot-stale",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        );
      }
    },
  );

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
        if (result._tag === "Failure" || result.success.exitCode !== 0) {
          return yield* error("repository-unavailable", operation, projectId, taskId);
        }
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
          Effect.mapError((failure) =>
            error(
              failure.exitCode === undefined
                ? "repository-unavailable"
                : "default-remote-ref-unavailable",
              operation,
              projectId,
              taskId,
            ),
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
      if (remote.exitCode !== 0) {
        return yield* error(
          "repository-unavailable",
          operation,
          state.projectId,
          state.taskId,
          state.reservationId,
        );
      }
      if (normalizeGitRemoteUrl(remote.stdout.trim()) === state.repository.canonicalKey) {
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
    if (remoteUrl.exitCode !== 0) {
      return yield* error(
        "repository-unavailable",
        operation,
        state.projectId,
        state.taskId,
        state.reservationId,
      );
    }
    return (
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
    reservedTarget: AgentControlWorktreeTargetIdentity | null = null,
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
          cause.reason === "observation-failed"
            ? "repository-unavailable"
            : cause.reason === "root-invalid" || cause.reason === "target-invalid"
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
      catch: () =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
    });
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
    const targetExists = yield* pathExistsNoFollow(state.internalWorktreePath).pipe(
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
    const targetEntry = pathMatches[0];
    const branchEntry = branchMatches[0];
    if (branchEntry && path.resolve(branchEntry.path) !== state.internalWorktreePath) {
      return { _tag: "attention", code: "branch-in-other-worktree" };
    }
    if (targetExists && targetEntry === undefined) {
      if (
        reservedTarget !== null &&
        reservedTarget.path === state.internalWorktreePath &&
        !requireOwnership
      ) {
        return branchSha === null ? { _tag: "create-new" } : { _tag: "create-existing" };
      }
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
    if (branch.exitCode !== 0 && branch.exitCode !== 1) {
      return yield* error(
        "repository-unavailable",
        "reconcile",
        state.projectId,
        state.taskId,
        state.reservationId,
      );
    }
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
    const statusRecords = yield* Effect.try({
      try: () => parseAgentControlPorcelainV2Status(status.stdout),
      catch: () =>
        error(
          "repository-unavailable",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
    });
    if (statusRecords.length !== 0) {
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
      const present = yield* pathExistsNoFollow(markerPath).pipe(
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
      if (present) return { _tag: "attention", code: "worktree-sequencer-state" };
    }
    let ownershipFingerprint: string | null = null;
    if (requireOwnership) {
      const markerPath = yield* ownershipMarkerPath(state.internalWorktreePath, actualGitDir).pipe(
        Effect.provideService(Path.Path, path),
      );
      const gitDirInfo = yield* fs
        .stat(actualGitDir)
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
      const markerResult = yield* Effect.result(
        inspectAgentControlWorktreeOwnershipMarker({
          markerPath,
          expectedDevice: gitDirInfo.dev,
          expectedUid: typeof process.getuid === "function" ? process.getuid() : null,
        }),
      );
      if (markerResult._tag === "Failure") {
        if (markerResult.failure.reason === "missing") {
          return { _tag: "attention", code: "ownership-unproven" };
        }
        if (
          markerResult.failure.reason === "io" ||
          markerResult.failure.reason === "incomplete" ||
          markerResult.failure.reason === "corrupt"
        ) {
          return yield* error(
            "repository-unavailable",
            "reconcile",
            state.projectId,
            state.taskId,
            state.reservationId,
          );
        }
        return { _tag: "attention", code: "ownership-mismatch" };
      }
      const expected = expectedAgentControlWorktreeOwnershipMarker(state);
      ownershipFingerprint = fingerprintAgentControlWorktreeOwnership(markerResult.success);
      if (
        ownershipFingerprint !== fingerprintAgentControlWorktreeOwnership(expected) ||
        (state.ownershipFingerprint !== null && state.ownershipFingerprint !== ownershipFingerprint)
      ) {
        return { _tag: "attention", code: "ownership-mismatch" };
      }
    }
    yield* revalidateAgentControlWorktreePathIdentity(pathIdentity).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.mapError((cause) =>
        error(
          cause.reason === "observation-failed"
            ? "repository-unavailable"
            : "worktree-path-invalid",
          "reconcile",
          state.projectId,
          state.taskId,
          state.reservationId,
        ),
      ),
    );
    return { _tag: "exact", ownershipFingerprint, pathIdentity, gitDir: actualGitDir };
  });

  const markAttention = Effect.fn("AgentControlWorktreeController.markAttention")(function* (
    baseCommandId: CommandId,
    state: AgentControlWorktreeReservationState,
    code: AgentControlWorktreeAttentionCode,
    claim: CompositeClaim,
    targetResource: TargetResource | null,
  ) {
    const materializationPhase = claim.row.materializationPhase;
    if (
      materializationPhase !== "reserved" &&
      materializationPhase !== "materializing" &&
      materializationPhase !== "git-created" &&
      materializationPhase !== "ownership-marked"
    ) {
      return yield* error(
        "lease-recovery-required",
        "reconcile",
        state.projectId,
        state.taskId,
        state.reservationId,
      );
    }
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (materializationPhase === "git-created" || materializationPhase === "ownership-marked") {
          if (targetResource === null) {
            return yield* error(
              "lease-recovery-required",
              "reconcile",
              state.projectId,
              state.taskId,
              state.reservationId,
            );
          }
          yield* completeTargetResource(claim, targetResource, state, "retained-attention");
        } else if (targetResource !== null) {
          if (targetResource.cleanupActive === null) {
            return yield* error(
              "repository-unavailable",
              "reconcile",
              state.projectId,
              state.taskId,
              state.reservationId,
            );
          }
          yield* cleanupTargetClaim(claim, targetResource.row, targetResource.identity, state);
          yield* Ref.set(targetResource.cleanupActive, false);
        }
        return yield* accepted({
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
          materializationPhase,
          gitCreatedDevice: claim.row.gitCreatedDevice,
          gitCreatedInode: claim.row.gitCreatedInode,
          gitCreatedGitDir: claim.row.gitCreatedGitDir,
          markedOwnershipFingerprint: claim.row.markedOwnershipFingerprint,
        });
      }),
    );
  });

  const materialize = Effect.fn("AgentControlWorktreeController.materialize")(function* (
    initialClaim: CompositeClaim,
    initial: AgentControlWorktreeReservationState,
    claimOwner: Ref.Ref<CompositeClaim | null>,
  ) {
    const baseCommandId = initialClaim.commandId;
    const lock = yield* getLock(initial.repositoryCommonDir);
    return yield* lock.withPermit(
      Effect.scoped(
        withAgentControlRepositoryLock({
          repositoryCommonDir: initial.repositoryCommonDir,
          runtimeHolderId: holderId,
          effect: Effect.gen(function* () {
            let claim = initialClaim;
            let state = (yield* engine.loadAuthoritative(initial.reservationId)) ?? initial;
            if (
              claim.row.worktreeReservationId !== state.reservationId ||
              claim.row.targetGenerationId !== state.targetGenerationId
            ) {
              return yield* error(
                "reservation-conflict",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            if (state.status === "ready" || state.status === "needs-attention") {
              return { state, claim };
            }
            let canonical = yield* preflight(state.projectId, state.taskId, "reconcile");
            yield* lifecycleCheckpoint("after-preflight", claim.commandId, state.reservationId);
            const initialAuthorityFingerprint = authorityFingerprint(canonical);
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
            yield* ensureCanonicalBinding(canonical, state, "reconcile");
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
            if (
              claim.row.materializationPhase !== "reserved" &&
              claim.row.materializationPhase !== "materializing" &&
              claim.row.materializationPhase !== "git-created" &&
              claim.row.materializationPhase !== "ownership-marked"
            ) {
              return yield* error(
                "lease-recovery-required",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }

            const pathIdentity = yield* validateExistingAgentControlWorktreePath({
              target: state.internalWorktreePath,
              repositoryWorkspace: state.repositoryWorkspace,
            }).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
              Effect.provideService(ServerConfig, serverConfig),
              Effect.mapError((cause) =>
                error(
                  cause.reason === "observation-failed"
                    ? "repository-unavailable"
                    : cause.reason === "root-invalid" || cause.reason === "target-invalid"
                      ? "internal-persistence-error"
                      : "worktree-path-invalid",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                ),
              ),
            );
            let targetResource = yield* recoverTargetResource(claim, state, pathIdentity);
            let observation = yield* inspect(
              state,
              canonical,
              false,
              targetResource?.empty === true ? targetResource.identity : null,
            );
            if (observation._tag === "attention") {
              return {
                state: yield* markAttention(
                  baseCommandId,
                  state,
                  observation.code,
                  claim,
                  targetResource,
                ),
                claim,
              };
            }
            if (observation._tag !== "exact") {
              if (
                claim.row.materializationPhase !== "reserved" &&
                claim.row.materializationPhase !== "materializing"
              ) {
                return yield* error(
                  "lease-recovery-required",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                );
              }
              yield* revalidateAgentControlWorktreePathIdentity(pathIdentity).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.mapError((cause) =>
                  error(
                    cause.reason === "observation-failed"
                      ? "repository-unavailable"
                      : "worktree-path-invalid",
                    "reconcile",
                    state.projectId,
                    state.taskId,
                    state.reservationId,
                  ),
                ),
              );
              if (!(yield* inspectRepositoryIdentity(canonical, state, "reconcile"))) {
                return {
                  state: yield* markAttention(
                    baseCommandId,
                    state,
                    "repository-identity-mismatch",
                    claim,
                    targetResource,
                  ),
                  claim,
                };
              }
              if (targetResource === null) {
                targetResource = yield* acquireTargetResource(claim, state, pathIdentity);
              }
              if (claim.row.materializationPhase === "reserved") {
                claim = yield* advanceOwnedClaim(
                  claimOwner,
                  transitionCompositePhase({
                    claim,
                    from: "reserved",
                    to: "materializing",
                    state,
                  }),
                );
              }
              yield* lifecycleCheckpoint(
                "after-materializing",
                claim.commandId,
                state.reservationId,
              );
              yield* verifyAgentControlWorktreeTargetPath(
                pathIdentity,
                targetResource.identity,
              ).pipe(
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
              const createResult = yield* Effect.result(
                workflow.createWorktree({
                  cwd: state.repositoryWorkspace,
                  refName:
                    observation._tag === "create-new" ? state.baseCommitSha : state.branchName,
                  ...(observation._tag === "create-new"
                    ? { newRefName: state.branchName, baseRefName: state.baseRef }
                    : {}),
                  path: state.internalWorktreePath,
                }),
              );
              if (createResult._tag === "Success") {
                if (targetResource.cleanupActive !== null) {
                  yield* Effect.uninterruptible(Ref.set(targetResource.cleanupActive, false));
                }
                yield* lifecycleCheckpoint("after-git-call", claim.commandId, state.reservationId);
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
                const gitDirOutput = yield* gitRun(
                  "AgentControlWorktree.materialize.gitDir",
                  state.internalWorktreePath,
                  ["rev-parse", "--git-dir"],
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
                const gitDirCandidate = path.isAbsolute(gitDirOutput.stdout.trim())
                  ? gitDirOutput.stdout.trim()
                  : path.resolve(state.internalWorktreePath, gitDirOutput.stdout.trim());
                const materializedGitDir = yield* fs
                  .realPath(gitDirCandidate)
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
                if (
                  materializedTargetInode === undefined ||
                  materializedTargetInfo.dev !== targetResource.identity.device ||
                  materializedTargetInode !== targetResource.identity.inode
                ) {
                  return yield* error(
                    "repository-unavailable",
                    "reconcile",
                    state.projectId,
                    state.taskId,
                    state.reservationId,
                  );
                }
                yield* revalidateAgentControlWorktreePathIdentity(pathIdentity).pipe(
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
                claim = yield* advanceOwnedClaim(
                  claimOwner,
                  transitionCompositePhase({
                    claim,
                    from: "materializing",
                    to: "git-created",
                    state,
                    gitCreatedDevice: materializedTargetInfo.dev,
                    gitCreatedInode: materializedTargetInode,
                    gitCreatedGitDir: materializedGitDir,
                  }),
                );
                yield* lifecycleCheckpoint(
                  "after-git-created",
                  claim.commandId,
                  state.reservationId,
                );
              }
              const afterCreate = yield* inspect(state, canonical, false, targetResource.identity);
              if (createResult._tag === "Failure") {
                if (afterCreate._tag === "attention") {
                  return {
                    state: yield* markAttention(
                      baseCommandId,
                      state,
                      afterCreate.code,
                      claim,
                      targetResource,
                    ),
                    claim,
                  };
                }
                if (afterCreate._tag !== "exact") {
                  return yield* error(
                    "repository-unavailable",
                    "reconcile",
                    state.projectId,
                    state.taskId,
                    state.reservationId,
                  );
                }
              }
              if (afterCreate._tag !== "exact") {
                if (afterCreate._tag === "attention") {
                  return {
                    state: yield* markAttention(
                      baseCommandId,
                      state,
                      afterCreate.code,
                      claim,
                      targetResource,
                    ),
                    claim,
                  };
                }
                return yield* error(
                  "repository-unavailable",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                );
              }
              observation = afterCreate;
            }

            if (claim.row.materializationPhase === "reserved" && observation._tag === "exact") {
              return {
                state: yield* markAttention(
                  baseCommandId,
                  state,
                  "ownership-unproven",
                  claim,
                  targetResource,
                ),
                claim,
              };
            }
            if (
              claim.row.materializationPhase === "materializing" &&
              observation._tag === "exact"
            ) {
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
                materializedTargetInode === undefined ||
                (targetResource !== null &&
                  (materializedTargetInfo.dev !== targetResource.identity.device ||
                    materializedTargetInode !== targetResource.identity.inode))
              ) {
                return yield* error(
                  "repository-unavailable",
                  "reconcile",
                  state.projectId,
                  state.taskId,
                  state.reservationId,
                );
              }
              yield* revalidateAgentControlWorktreePathIdentity(observation.pathIdentity).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.mapError((cause) =>
                  error(
                    cause.reason === "observation-failed"
                      ? "repository-unavailable"
                      : "worktree-path-invalid",
                    "reconcile",
                    state.projectId,
                    state.taskId,
                    state.reservationId,
                  ),
                ),
              );
              claim = yield* advanceOwnedClaim(
                claimOwner,
                transitionCompositePhase({
                  claim,
                  from: "materializing",
                  to: "git-created",
                  state,
                  gitCreatedDevice: materializedTargetInfo.dev,
                  gitCreatedInode: materializedTargetInode,
                  gitCreatedGitDir: observation.gitDir,
                }),
              );
              yield* lifecycleCheckpoint("after-git-created", claim.commandId, state.reservationId);
            }

            if (observation._tag !== "exact") {
              return yield* error(
                "repository-unavailable",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            const targetInfo = yield* fs
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
            const targetInode = Option.getOrUndefined(targetInfo.ino);
            if (
              claim.row.gitCreatedDevice !== targetInfo.dev ||
              claim.row.gitCreatedInode !== targetInode ||
              claim.row.gitCreatedGitDir !== observation.gitDir
            ) {
              return {
                state: yield* markAttention(
                  baseCommandId,
                  state,
                  "repository-identity-mismatch",
                  claim,
                  targetResource,
                ),
                claim,
              };
            }

            let owned = yield* inspect(state, canonical, true);
            if (
              claim.row.materializationPhase === "git-created" &&
              owned._tag === "attention" &&
              owned.code === "ownership-unproven"
            ) {
              const markerPath = yield* ownershipMarkerPath(
                state.internalWorktreePath,
                observation.gitDir,
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
              yield* lifecycleCheckpoint(
                "after-marker-publish",
                claim.commandId,
                state.reservationId,
              );
              owned = yield* inspect(state, canonical, true);
            }
            if (owned._tag !== "exact" || owned.ownershipFingerprint === null) {
              return {
                state: yield* markAttention(
                  baseCommandId,
                  state,
                  owned._tag === "attention" ? owned.code : "worktree-registration-mismatch",
                  claim,
                  targetResource,
                ),
                claim,
              };
            }
            if (claim.row.materializationPhase === "git-created") {
              claim = yield* advanceOwnedClaim(
                claimOwner,
                transitionCompositePhase({
                  claim,
                  from: "git-created",
                  to: "ownership-marked",
                  state,
                  ownershipFingerprint: owned.ownershipFingerprint,
                }),
              );
              yield* lifecycleCheckpoint(
                "after-ownership-marked",
                claim.commandId,
                state.reservationId,
              );
            } else if (
              claim.row.materializationPhase !== "ownership-marked" ||
              claim.row.markedOwnershipFingerprint !== owned.ownershipFingerprint
            ) {
              return yield* error(
                "lease-recovery-required",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }

            canonical = yield* preflight(state.projectId, state.taskId, "reconcile");
            yield* ensureCanonicalBinding(canonical, state, "reconcile");
            if (authorityFingerprint(canonical) !== initialAuthorityFingerprint) {
              return yield* error(
                "source-snapshot-stale",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            const finalObservation = yield* inspect(state, canonical, true);
            if (
              finalObservation._tag !== "exact" ||
              finalObservation.ownershipFingerprint === null
            ) {
              return {
                state: yield* markAttention(
                  baseCommandId,
                  state,
                  finalObservation._tag === "attention"
                    ? finalObservation.code
                    : "worktree-registration-mismatch",
                  claim,
                  targetResource,
                ),
                claim,
              };
            }
            const verifiedAt = DateTime.formatIso(yield* DateTime.now);
            yield* lifecycleCheckpoint("before-ready", claim.commandId, state.reservationId);
            if (
              targetResource === null ||
              claim.row.gitCreatedDevice === null ||
              claim.row.gitCreatedInode === null ||
              claim.row.gitCreatedGitDir === null ||
              claim.row.markedOwnershipFingerprint === null
            ) {
              return yield* error(
                "lease-recovery-required",
                "reconcile",
                state.projectId,
                state.taskId,
                state.reservationId,
              );
            }
            state = yield* Effect.uninterruptible(
              completeTargetResource(claim, targetResource, state).pipe(
                Effect.andThen(
                  accepted({
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
                    gitCreatedDevice: claim.row.gitCreatedDevice,
                    gitCreatedInode: claim.row.gitCreatedInode,
                    gitCreatedGitDir: claim.row.gitCreatedGitDir,
                    markedOwnershipFingerprint: claim.row.markedOwnershipFingerprint,
                    verifiedAt,
                  }),
                ),
              ),
            );
            targetResource = null;
            return { state, claim };
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
      ),
    );
  });

  const reserveAndMaterialize: AgentControlWorktreeControllerShape["reserveAndMaterialize"] = (
    input,
  ) =>
    getOperationLock(input.commandId).pipe(
      Effect.flatMap((operationLock) =>
        operationLock.withPermit(
          runCompositeLifecycle(
            {
              ...input,
              commandType: "reserve-and-materialize",
            },
            "reserve",
            (initialClaim, claimOwner) =>
              Effect.gen(function* () {
                let claim = initialClaim;
                yield* controllerHooks.afterCompositeClaim?.(claim.commandId) ?? Effect.void;
                let preboundReservationId:
                  | AgentControlWorktreeReservationState["reservationId"]
                  | null = null;
                if (claim.row.worktreeReservationId !== null) {
                  preboundReservationId = claim.row
                    .worktreeReservationId as AgentControlWorktreeReservationState["reservationId"];
                  const mapped = yield* engine.loadAuthoritative(preboundReservationId);
                  if (mapped !== null) {
                    if (mapped.projectId !== input.projectId || mapped.taskId !== input.taskId) {
                      return yield* error(
                        "reservation-projection-corrupt",
                        "reserve",
                        input.projectId,
                        input.taskId,
                      );
                    }
                    return yield* materialize(claim, mapped, claimOwner);
                  }
                  if (
                    claim.row.materializationPhase !== "reserved" ||
                    claim.row.targetGenerationId === null
                  ) {
                    return yield* error(
                      "reservation-projection-corrupt",
                      "reserve",
                      input.projectId,
                      input.taskId,
                    );
                  }
                }
                const canonical = yield* preflight(input.projectId, input.taskId, "reserve");
                yield* lifecycleCheckpoint("after-preflight", claim.commandId, null);
                const existingProjection = yield* states
                  .getByStage({
                    projectId: input.projectId,
                    taskId: input.taskId,
                    stageRunId: canonical.stageRun.stageRunId,
                    attemptId: canonical.stageRun.attemptId,
                  })
                  .pipe(
                    Effect.mapError(() =>
                      error(
                        "reservation-projection-corrupt",
                        "reserve",
                        input.projectId,
                        input.taskId,
                      ),
                    ),
                  );
                if (Option.isSome(existingProjection)) {
                  const projected = existingProjection.value;
                  if (
                    preboundReservationId !== null &&
                    preboundReservationId !== projected.reservationId
                  ) {
                    return yield* error(
                      "reservation-projection-corrupt",
                      "reserve",
                      input.projectId,
                      input.taskId,
                      preboundReservationId,
                    );
                  }
                  if (
                    projected.taskRevision !== canonical.task.revision ||
                    projected.githubIntakeSequence !== canonical.task.githubIntakeSequence ||
                    projected.sourceIdentityFingerprint !== canonical.sourceIdentityFingerprint ||
                    projected.repository.repositoryNodeId !==
                      canonical.repository.repositoryNodeId ||
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
                  claim = yield* advanceOwnedClaim(
                    claimOwner,
                    bindCompositeReservation(
                      claim,
                      authoritative.reservationId,
                      authoritative.targetGenerationId,
                      "reserve",
                      input.projectId,
                      input.taskId,
                    ),
                  );
                  return yield* materialize(claim, authoritative, claimOwner);
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
                    error("repository-unavailable", "reserve", input.projectId, input.taskId),
                  ),
                );
                if (validBranch.exitCode !== 0) {
                  return yield* error(
                    "branch-name-invalid",
                    "reserve",
                    input.projectId,
                    input.taskId,
                  );
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
                if (preboundReservationId !== null && preboundReservationId !== reservationId) {
                  return yield* error(
                    "reservation-projection-corrupt",
                    "reserve",
                    input.projectId,
                    input.taskId,
                    preboundReservationId,
                  );
                }
                if (preboundReservationId === null) {
                  if (claim.row.targetGenerationId === null) {
                    return yield* error(
                      "internal-persistence-error",
                      "reserve",
                      input.projectId,
                      input.taskId,
                      reservationId,
                    );
                  }
                  claim = yield* advanceOwnedClaim(
                    claimOwner,
                    bindCompositeReservation(
                      claim,
                      reservationId,
                      claim.row.targetGenerationId,
                      "reserve",
                      input.projectId,
                      input.taskId,
                    ),
                  );
                }
                if (claim.row.targetGenerationId === null) {
                  return yield* error(
                    "internal-persistence-error",
                    "reserve",
                    input.projectId,
                    input.taskId,
                    reservationId,
                  );
                }
                const safePath = yield* deriveSafeAgentControlWorktreePath({
                  projectId: input.projectId,
                  reservationId,
                  targetGenerationId: claim.row.targetGenerationId,
                  repositoryWorkspace: repository.repositoryWorkspace,
                }).pipe(
                  Effect.provideService(FileSystem.FileSystem, fs),
                  Effect.provideService(Path.Path, path),
                  Effect.provideService(ServerConfig, serverConfig),
                  Effect.mapError((cause) =>
                    error(
                      cause.reason === "observation-failed"
                        ? "repository-unavailable"
                        : cause.reason === "target-exists"
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
                  targetGenerationId: claim.row.targetGenerationId,
                  worktreeRootDevice: safePath.rootIdentity.device,
                  worktreeRootInode: safePath.rootIdentity.inode,
                  worktreeParentDevice: safePath.parentIdentity.device,
                  worktreeParentInode: safePath.parentIdentity.inode,
                });
                yield* lifecycleCheckpoint("after-reserved", claim.commandId, state.reservationId);
                return yield* materialize(claim, state, claimOwner);
              }),
          ),
        ),
      ),
    );

  const reconcile: AgentControlWorktreeControllerShape["reconcile"] = (input) =>
    getOperationLock(input.commandId).pipe(
      Effect.flatMap((operationLock) =>
        operationLock.withPermit(
          runCompositeLifecycle(
            {
              ...input,
              commandType: "reconcile",
            },
            "reconcile",
            (initialClaim, claimOwner) =>
              Effect.gen(function* () {
                let claim = initialClaim;
                yield* controllerHooks.afterCompositeClaim?.(claim.commandId) ?? Effect.void;
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
                if (claim.row.targetGenerationId !== state.targetGenerationId) {
                  claim = yield* advanceOwnedClaim(
                    claimOwner,
                    bindCompositeReservation(
                      claim,
                      state.reservationId,
                      state.targetGenerationId,
                      "reconcile",
                      state.projectId,
                      state.taskId,
                    ),
                  );
                }
                return yield* materialize(claim, state, claimOwner);
              }),
          ),
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
            yield* ensureCanonicalBinding(canonical, authoritative, "materialize");
            const initialAuthorityFingerprint = authorityFingerprint(canonical);
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
            yield* controllerHooks.afterReadyInspection(authoritative.reservationId);
            const secondAuthoritative = yield* engine.loadAuthoritative(input.reservationId);
            if (
              secondAuthoritative === null ||
              secondAuthoritative.projectId !== authoritative.projectId ||
              secondAuthoritative.revision !== authoritative.revision ||
              secondAuthoritative.sequence !== authoritative.sequence ||
              secondAuthoritative.status !== "ready" ||
              secondAuthoritative.ownershipFingerprint !== authoritative.ownershipFingerprint
            ) {
              return yield* error(
                "state-not-available",
                "materialize",
                input.projectId,
                authoritative.taskId,
                input.reservationId,
              );
            }
            const secondCanonical = yield* preflight(
              authoritative.projectId,
              authoritative.taskId,
              "materialize",
            );
            yield* ensureCanonicalBinding(secondCanonical, authoritative, "materialize");
            if (authorityFingerprint(secondCanonical) !== initialAuthorityFingerprint) {
              return yield* error(
                "source-snapshot-stale",
                "materialize",
                input.projectId,
                authoritative.taskId,
                input.reservationId,
              );
            }
            return yield* Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const callbackFiber = yield* Effect.scoped(callback(authoritative)).pipe(
                  Effect.forkChild({ startImmediately: true }),
                );
                return yield* restore(Fiber.join(callbackFiber)).pipe(
                  Effect.onExit(() => Fiber.interrupt(callbackFiber).pipe(Effect.asVoid)),
                );
              }),
            );
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
