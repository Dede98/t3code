import {
  AgentControlWorktreeReservationId,
  AgentControlWorktreeReservationState,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";
import { validateAgentControlWorktreeReservationState } from "../invariant.ts";
import {
  AgentControlWorktreeStateRepository,
  type AgentControlWorktreeEnumerationEntry,
  type AgentControlWorktreeStateRepositoryShape,
} from "../Services/AgentControlWorktreeStateRepository.ts";

const PersistedRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: Schema.String,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  fenceToken: Schema.Number,
  repositoryNodeId: Schema.String,
  repositoryNameWithOwner: Schema.String,
  repositoryCanonicalKey: Schema.String,
  repositoryRemoteName: Schema.String,
  repositoryRemoteUrl: Schema.String,
  repositoryDefaultRemoteRef: Schema.String,
  repositoryCommonDirDevice: Schema.Number,
  repositoryCommonDirInode: Schema.Number,
  repositoryWorkspace: Schema.String,
  repositoryCommonDir: Schema.String,
  baseRef: Schema.String,
  baseCommitSha: Schema.String,
  branchName: Schema.String,
  internalWorktreePath: Schema.String,
  targetGenerationId: Schema.String,
  worktreeRootDevice: Schema.Number,
  worktreeRootInode: Schema.Number,
  worktreeParentDevice: Schema.Number,
  worktreeParentInode: Schema.Number,
  materializationPhase: Schema.String,
  gitCreatedDevice: Schema.NullOr(Schema.Number),
  gitCreatedInode: Schema.NullOr(Schema.Number),
  gitCreatedGitDir: Schema.NullOr(Schema.String),
  markedOwnershipFingerprint: Schema.NullOr(Schema.String),
  headCommitSha: Schema.NullOr(Schema.String),
  ownershipFingerprint: Schema.NullOr(Schema.String),
  verifiedAt: Schema.NullOr(Schema.String),
  status: Schema.String,
  attentionCode: Schema.NullOr(Schema.String),
  state: Schema.fromJsonString(AgentControlWorktreeReservationState),
  revision: Schema.Number,
  sequence: Schema.Number,
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
const decodeRow = Schema.decodeUnknownEffect(PersistedRow);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);
const decodeProjectId = Schema.decodeUnknownEffect(ProjectId);
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeReservationState),
);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });
const SELECT = `
  reservation_id AS "reservationId", project_id AS "projectId", task_id AS "taskId",
  task_revision AS "taskRevision", github_intake_sequence AS "githubIntakeSequence",
  source_identity_fingerprint AS "sourceIdentityFingerprint",
  stage_run_id AS "stageRunId", attempt_id AS "attemptId", lease_id AS "leaseId",
  fence_token AS "fenceToken", repository_node_id AS "repositoryNodeId",
  repository_name_with_owner AS "repositoryNameWithOwner",
  repository_canonical_key AS "repositoryCanonicalKey",
  repository_remote_name AS "repositoryRemoteName",
  repository_remote_url AS "repositoryRemoteUrl",
  repository_default_remote_ref AS "repositoryDefaultRemoteRef",
  repository_common_dir_device AS "repositoryCommonDirDevice",
  repository_common_dir_inode AS "repositoryCommonDirInode",
  repository_workspace AS "repositoryWorkspace",
  repository_common_dir AS "repositoryCommonDir", base_ref AS "baseRef",
    base_commit_sha AS "baseCommitSha", branch_name AS "branchName",
    internal_worktree_path AS "internalWorktreePath",
    target_generation_id AS "targetGenerationId", head_commit_sha AS "headCommitSha",
  worktree_root_device AS "worktreeRootDevice",
  worktree_root_inode AS "worktreeRootInode",
    worktree_parent_device AS "worktreeParentDevice",
    worktree_parent_inode AS "worktreeParentInode",
    materialization_phase AS "materializationPhase",
    git_created_device AS "gitCreatedDevice", git_created_inode AS "gitCreatedInode",
    git_created_git_dir AS "gitCreatedGitDir",
    marked_ownership_fingerprint AS "markedOwnershipFingerprint",
  ownership_fingerprint AS "ownershipFingerprint", verified_at AS "verifiedAt",
  status, attention_code AS "attentionCode", state_json AS state, revision,
  last_event_sequence AS sequence, created_at AS "createdAt", updated_at AS "updatedAt"
`;

const decodeInvariant = Effect.fn("AgentControlWorktreeStateRepository.decodeInvariant")(function* (
  row: Record<string, unknown>,
  operation: string,
) {
  const decoded = yield* decodeRow(row).pipe(
    Effect.mapError((cause) => decodeError(`${operation}:decode`, cause)),
  );
  const state = yield* validateAgentControlWorktreeReservationState(decoded.state).pipe(
    Effect.mapError((cause) => decodeError(`${operation}:invariant`, cause)),
  );
  if (
    decoded.reservationId !== state.reservationId ||
    decoded.projectId !== state.projectId ||
    decoded.taskId !== state.taskId ||
    decoded.taskRevision !== state.taskRevision ||
    decoded.githubIntakeSequence !== state.githubIntakeSequence ||
    decoded.sourceIdentityFingerprint !== state.sourceIdentityFingerprint ||
    decoded.stageRunId !== state.stageRunId ||
    decoded.attemptId !== state.attemptId ||
    decoded.leaseId !== state.leaseId ||
    decoded.fenceToken !== state.fenceToken ||
    decoded.repositoryNodeId !== state.repository.repositoryNodeId ||
    decoded.repositoryNameWithOwner !== state.repository.nameWithOwner ||
    decoded.repositoryCanonicalKey !== state.repository.canonicalKey ||
    decoded.repositoryRemoteName !== state.repository.remoteName ||
    decoded.repositoryRemoteUrl !== state.repository.remoteUrl ||
    decoded.repositoryDefaultRemoteRef !== state.repository.defaultRemoteRef ||
    decoded.repositoryCommonDirDevice !== state.repository.commonDirDevice ||
    decoded.repositoryCommonDirInode !== state.repository.commonDirInode ||
    decoded.repositoryWorkspace !== state.repositoryWorkspace ||
    decoded.repositoryCommonDir !== state.repositoryCommonDir ||
    decoded.baseRef !== state.baseRef ||
    decoded.baseCommitSha !== state.baseCommitSha ||
    decoded.branchName !== state.branchName ||
    decoded.internalWorktreePath !== state.internalWorktreePath ||
    decoded.targetGenerationId !== state.targetGenerationId ||
    decoded.worktreeRootDevice !== state.worktreeRootDevice ||
    decoded.worktreeRootInode !== state.worktreeRootInode ||
    decoded.worktreeParentDevice !== state.worktreeParentDevice ||
    decoded.worktreeParentInode !== state.worktreeParentInode ||
    decoded.materializationPhase !== state.materializationPhase ||
    decoded.gitCreatedDevice !== state.gitCreatedDevice ||
    decoded.gitCreatedInode !== state.gitCreatedInode ||
    decoded.gitCreatedGitDir !== state.gitCreatedGitDir ||
    decoded.markedOwnershipFingerprint !== state.markedOwnershipFingerprint ||
    decoded.headCommitSha !== state.headCommitSha ||
    decoded.ownershipFingerprint !== state.ownershipFingerprint ||
    decoded.verifiedAt !== state.verifiedAt ||
    decoded.status !== state.status ||
    decoded.attentionCode !== state.attentionCode ||
    decoded.revision !== state.revision ||
    decoded.sequence !== state.sequence ||
    decoded.createdAt !== state.createdAt ||
    decoded.updatedAt !== state.updatedAt
  ) {
    return yield* decodeError(`${operation}:columns`, new Error("projection column mismatch"));
  }
  return state;
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlWorktreeStateRepositoryShape["get"] = (reservationId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE reservation_id = ?`,
        [reservationId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.get:query", cause),
        ),
        Effect.flatMap((rows) => {
          const row = rows[0];
          return row === undefined
            ? Effect.succeed(Option.none())
            : decodeInvariant(row, "AgentControlWorktreeStateRepository.get").pipe(
                Effect.map(Option.some),
              );
        }),
      );

  const getByStage: AgentControlWorktreeStateRepositoryShape["getByStage"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE project_id = ? AND task_id = ? AND stage_run_id = ? AND attempt_id = ?`,
        [input.projectId, input.taskId, input.stageRunId, input.attemptId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.getByStage:query", cause),
        ),
        Effect.flatMap((rows) => {
          if (rows.length > 1) {
            return Effect.fail(
              decodeError(
                "AgentControlWorktreeStateRepository.getByStage:ambiguous",
                new Error("multiple reservations for canonical stage attempt"),
              ),
            );
          }
          const row = rows[0];
          return row === undefined
            ? Effect.succeed(Option.none())
            : decodeInvariant(row, "AgentControlWorktreeStateRepository.getByStage").pipe(
                Effect.map(Option.some),
              );
        }),
      );

  const save: AgentControlWorktreeStateRepositoryShape["save"] = (rawState, expectedRevision) =>
    Effect.gen(function* () {
      const state = yield* validateAgentControlWorktreeReservationState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlWorktreeStateRepository.save:invariant", cause),
        ),
      );
      if (state.revision !== expectedRevision + 1) {
        return yield* decodeError(
          "AgentControlWorktreeStateRepository.save:revision",
          new Error("projection revision mismatch"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlWorktreeStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* sql<{ readonly reservationId: unknown }>`
              INSERT INTO agent_control_worktree_reservation_states (
                reservation_id, project_id, task_id, stage_run_id, attempt_id,
                task_revision, github_intake_sequence, source_identity_fingerprint,
                lease_id, fence_token, repository_node_id, repository_name_with_owner,
                repository_canonical_key, repository_remote_name, repository_remote_url,
                repository_default_remote_ref, repository_common_dir_device,
                repository_common_dir_inode, repository_workspace, repository_common_dir,
                base_ref, base_commit_sha, branch_name, internal_worktree_path,
                target_generation_id,
                worktree_root_device, worktree_root_inode,
                worktree_parent_device, worktree_parent_inode,
                materialization_phase, git_created_device, git_created_inode,
                git_created_git_dir, marked_ownership_fingerprint,
                head_commit_sha, ownership_fingerprint, verified_at, status,
                attention_code, state_json, revision, last_event_sequence, created_at, updated_at
              ) VALUES (
                ${state.reservationId}, ${state.projectId}, ${state.taskId},
                ${state.stageRunId}, ${state.attemptId}, ${state.taskRevision},
                ${state.githubIntakeSequence}, ${state.sourceIdentityFingerprint}, ${state.leaseId},
                ${state.fenceToken}, ${state.repository.repositoryNodeId},
                ${state.repository.nameWithOwner}, ${state.repository.canonicalKey},
                ${state.repository.remoteName}, ${state.repository.remoteUrl},
                ${state.repository.defaultRemoteRef}, ${state.repository.commonDirDevice},
                ${state.repository.commonDirInode}, ${state.repositoryWorkspace},
                ${state.repositoryCommonDir}, ${state.baseRef}, ${state.baseCommitSha},
                ${state.branchName}, ${state.internalWorktreePath},
                ${state.targetGenerationId},
                ${state.worktreeRootDevice}, ${state.worktreeRootInode},
                ${state.worktreeParentDevice}, ${state.worktreeParentInode},
                ${state.materializationPhase}, ${state.gitCreatedDevice},
                ${state.gitCreatedInode}, ${state.gitCreatedGitDir},
                ${state.markedOwnershipFingerprint},
                ${state.headCommitSha},
                ${state.ownershipFingerprint}, ${state.verifiedAt}, ${state.status},
                ${state.attentionCode}, ${stateJson}, ${state.revision},
                ${state.sequence}, ${state.createdAt}, ${state.updatedAt}
              )
              ON CONFLICT (reservation_id) DO NOTHING
              RETURNING reservation_id AS "reservationId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlWorktreeStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<{ readonly reservationId: unknown }>`
              UPDATE agent_control_worktree_reservation_states SET
                status = ${state.status},
                attention_code = ${state.attentionCode},
                head_commit_sha = ${state.headCommitSha},
                ownership_fingerprint = ${state.ownershipFingerprint},
                verified_at = ${state.verifiedAt},
                materialization_phase = ${state.materializationPhase},
                git_created_device = ${state.gitCreatedDevice},
                git_created_inode = ${state.gitCreatedInode},
                git_created_git_dir = ${state.gitCreatedGitDir},
                marked_ownership_fingerprint = ${state.markedOwnershipFingerprint},
                state_json = ${stateJson},
                revision = ${state.revision},
                last_event_sequence = ${state.sequence},
                updated_at = ${state.updatedAt}
              WHERE reservation_id = ${state.reservationId}
                AND project_id = ${state.projectId}
                AND revision = ${expectedRevision}
              RETURNING reservation_id AS "reservationId"
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlWorktreeStateRepository.save:update", cause),
              ),
            );
      if (rows.length !== 1) {
        return yield* decodeError(
          "AgentControlWorktreeStateRepository.save:conflict",
          new Error("projection CAS conflict"),
        );
      }
    });

  const listProject: AgentControlWorktreeStateRepositoryShape["listProject"] = (projectId) =>
    sql
      .unsafe<Record<string, unknown>>(
        `SELECT ${SELECT}
         FROM agent_control_worktree_reservation_states
         WHERE project_id = ?
         ORDER BY created_at ASC, reservation_id ASC`,
        [projectId],
      )
      .pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlWorktreeStateRepository.listProject:query", cause),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            decodeInvariant(row, "AgentControlWorktreeStateRepository.listProject").pipe(
              Effect.matchEffect({
                onSuccess: (state) =>
                  Effect.succeed({
                    _tag: "Valid",
                    state,
                  } satisfies AgentControlWorktreeEnumerationEntry),
                onFailure: () =>
                  Effect.gen(function* () {
                    const reservationId = Option.getOrNull(
                      yield* Effect.option(decodeReservationId(row.reservationId)),
                    );
                    const rowProjectId = Option.getOrNull(
                      yield* Effect.option(decodeProjectId(row.projectId)),
                    );
                    return {
                      _tag: "Corrupt",
                      reservationId,
                      projectId: rowProjectId,
                    } satisfies AgentControlWorktreeEnumerationEntry;
                  }),
              }),
            ),
          ),
        ),
      );

  return AgentControlWorktreeStateRepository.of({
    get,
    getByStage,
    save,
    listProject,
    deleteAll: sql`DELETE FROM agent_control_worktree_reservation_states`.pipe(
      Effect.mapError((cause) => sqlError("AgentControlWorktreeStateRepository.deleteAll", cause)),
      Effect.asVoid,
    ),
  });
});

export const layer = Layer.effect(AgentControlWorktreeStateRepository, make);
