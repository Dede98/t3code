import {
  type AgentControlWorktreeReservationState,
  AgentControlWorktreeReservationId,
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  ProjectId,
  PositiveInt,
  NonNegativeInt,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { sha256FramedHex } from "./identity.ts";

export const AGENT_CONTROL_WORKTREE_OWNERSHIP_MARKER =
  "t3-agent-control-worktree-ownership-v1.json";

export const AgentControlWorktreeOwnershipMarker = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  repositoryNodeId: TrimmedNonEmptyString,
  repositoryCanonicalKey: TrimmedNonEmptyString,
  repositoryCommonDir: TrimmedNonEmptyString,
  repositoryCommonDirDevice: NonNegativeInt,
  repositoryCommonDirInode: NonNegativeInt,
  branchName: TrimmedNonEmptyString,
  baseCommitSha: TrimmedNonEmptyString,
  worktreeRootDevice: NonNegativeInt,
  worktreeRootInode: NonNegativeInt,
  worktreeParentDevice: NonNegativeInt,
  worktreeParentInode: NonNegativeInt,
});
export type AgentControlWorktreeOwnershipMarker = typeof AgentControlWorktreeOwnershipMarker.Type;

const decodeMarker = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeOwnershipMarker),
);
const encodeMarker = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeOwnershipMarker),
);

export const expectedAgentControlWorktreeOwnershipMarker = (
  state: AgentControlWorktreeReservationState,
): AgentControlWorktreeOwnershipMarker => ({
  schemaVersion: 1,
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
  repositoryNodeId: state.repository.repositoryNodeId,
  repositoryCanonicalKey: state.repository.canonicalKey,
  repositoryCommonDir: state.repositoryCommonDir,
  repositoryCommonDirDevice: state.repository.commonDirDevice,
  repositoryCommonDirInode: state.repository.commonDirInode,
  branchName: state.branchName,
  baseCommitSha: state.baseCommitSha,
  worktreeRootDevice: state.worktreeRootDevice,
  worktreeRootInode: state.worktreeRootInode,
  worktreeParentDevice: state.worktreeParentDevice,
  worktreeParentInode: state.worktreeParentInode,
});

export const fingerprintAgentControlWorktreeOwnership = (
  marker: AgentControlWorktreeOwnershipMarker,
) =>
  sha256FramedHex([
    "agent-control-worktree-ownership-v1",
    marker.reservationId,
    marker.projectId,
    marker.taskId,
    String(marker.taskRevision),
    String(marker.githubIntakeSequence),
    marker.sourceIdentityFingerprint,
    marker.stageRunId,
    marker.attemptId,
    marker.leaseId,
    String(marker.fenceToken),
    marker.repositoryNodeId,
    marker.repositoryCanonicalKey,
    marker.repositoryCommonDir,
    String(marker.repositoryCommonDirDevice),
    String(marker.repositoryCommonDirInode),
    marker.branchName,
    marker.baseCommitSha,
    String(marker.worktreeRootDevice),
    String(marker.worktreeRootInode),
    String(marker.worktreeParentDevice),
    String(marker.worktreeParentInode),
  ]);

export const ownershipMarkerPath = Effect.fn("ownershipMarkerPath")(function* (
  worktreePath: string,
  gitDirOutput: string,
) {
  const path = yield* Path.Path;
  const gitDir = gitDirOutput.trim();
  const absolute = path.isAbsolute(gitDir) ? gitDir : path.resolve(worktreePath, gitDir);
  return path.join(absolute, AGENT_CONTROL_WORKTREE_OWNERSHIP_MARKER);
});

export const readAgentControlWorktreeOwnershipMarker = Effect.fn(
  "readAgentControlWorktreeOwnershipMarker",
)(function* (markerPath: string) {
  const fs = yield* FileSystem.FileSystem;
  const contents = yield* fs.readFileString(markerPath);
  return yield* decodeMarker(contents);
});

export const encodeAgentControlWorktreeOwnershipMarker = encodeMarker;

/**
 * Publishes through an exclusive hard-link so an existing marker is never
 * replaced. A crash can leave only an unreferenced temporary file; it can
 * never expose a partially written ownership marker.
 */
export const writeAgentControlWorktreeOwnershipMarker = Effect.fn(
  "writeAgentControlWorktreeOwnershipMarker",
)(function* (markerPath: string, marker: AgentControlWorktreeOwnershipMarker) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const contents = `${yield* encodeMarker(marker)}\n`;
  const temporaryPath = path.join(
    path.dirname(markerPath),
    `.${AGENT_CONTROL_WORKTREE_OWNERSHIP_MARKER}.${process.pid}.${NodeCrypto.randomUUID()}.tmp`,
  );
  yield* Effect.addFinalizer(() => fs.remove(temporaryPath, { force: true }).pipe(Effect.ignore));
  const file = yield* fs.open(temporaryPath, { flag: "wx", mode: 0o600 });
  yield* file.writeAll(new TextEncoder().encode(contents));
  yield* file.sync;
  yield* fs.chmod(temporaryPath, 0o600);
  yield* fs.link(temporaryPath, markerPath);
  const directory = yield* fs.open(path.dirname(markerPath), { flag: "r" });
  yield* directory.sync;
  yield* fs.remove(temporaryPath);
});
