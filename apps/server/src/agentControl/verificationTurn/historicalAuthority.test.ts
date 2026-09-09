import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { canonicalJson, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  decodeAgentControlVerificationStoredTaskEvent,
  decodeAgentControlVerificationStoredWorktreeEvent,
} from "./historicalAuthority.ts";

const at = "2026-09-02T12:30:00.000Z";
const taskId = AgentControlTaskId.make("verification-history-task");
const projectId = ProjectId.make("verification-history-project");
const metadata = { schemaVersion: 1 } as const;
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const taskPayload = {
  taskId,
  source: {
    projectId,
    repositoryNodeId: "verification-history-repository",
    issueNodeId: "verification-history-issue",
    issueNumber: 1,
    issueUrl: "https://example.test/verification-history/1",
  },
  status: "candidate" as const,
  sourceGate: "eligible" as const,
  stage: "intake" as const,
  sourceUpdatedAt: at,
  githubIntakeSequence: 1,
  sourceSnapshot: {
    repositoryNodeId: "verification-history-repository",
    issueNodeId: "verification-history-issue",
    number: 1,
    url: "https://example.test/verification-history/1",
    state: "open" as const,
    title: "Verification history",
    body: null,
    contentTrust: "untrusted-external" as const,
    updatedAt: at,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible" as const,
  },
  createdAt: at,
};
const worktreePayload = {
  reservationId: AgentControlWorktreeReservationId.make("verification-history-worktree"),
  projectId,
  taskId,
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: AgentControlStageRunId.make("verification-history-stage"),
  attemptId: AgentControlAttemptId.make("verification-history-attempt"),
  leaseId: AgentControlStageRunLeaseId.make("verification-history-lease"),
  fenceToken: 1,
  repository: {
    repositoryNodeId: "verification-history-repository",
    nameWithOwner: "owner/repository",
    canonicalKey: "github.com/owner/repository",
    remoteName: "origin",
    remoteUrl: "github.com/owner/repository",
    defaultRemoteRef: "refs/remotes/origin/main",
    commonDirDevice: 1,
    commonDirInode: 1,
  },
  repositoryWorkspace: "/tmp/verification-history-repository",
  repositoryCommonDir: "/tmp/verification-history-repository/.git",
  baseRef: "origin/main",
  baseCommitSha: "b".repeat(40),
  branchName: "t3auto/issue-1-verification-history",
  internalWorktreePath: "/tmp/verification-history/worktree",
  targetGenerationId: "c".repeat(64),
  worktreeRootDevice: 1,
  worktreeRootInode: 1,
  worktreeParentDevice: 1,
  worktreeParentInode: 1,
  reservedAt: at,
};

const layer = it.layer(SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)));

layer("verification historical event bytes", (it) => {
  it.effect("accepts typed legacy/canonical Task and Worktree bytes and rejects alternatives", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const decode = Effect.fn("decodeVerificationHistoryFixture")(function* (
        kind: "task" | "worktree",
        payloadSource: string,
        metadataSource = encodeUnknownJson(metadata),
        payloadAsBlob = false,
      ) {
        const [storage] = yield* payloadAsBlob
          ? sql<Record<string, unknown>>`
              SELECT typeof(CAST(${payloadSource} AS BLOB)) AS "payloadStorageClass",
                CAST(${payloadSource} AS BLOB) AS "payloadBytes",
                typeof(${metadataSource}) AS "metadataStorageClass",
                CAST(${metadataSource} AS BLOB) AS "metadataBytes"
            `
          : sql<Record<string, unknown>>`
              SELECT typeof(${payloadSource}) AS "payloadStorageClass",
                CAST(${payloadSource} AS BLOB) AS "payloadBytes",
                typeof(${metadataSource}) AS "metadataStorageClass",
                CAST(${metadataSource} AS BLOB) AS "metadataBytes"
            `;
        const commandId = CommandId.make(`verification-history-${kind}-command`);
        const raw = {
          sequence: 1,
          eventId: EventId.make(`verification-history-${kind}-event`),
          aggregateKind: kind === "task" ? "task" : "worktree-reservation",
          aggregateId: kind === "task" ? taskId : worktreePayload.reservationId,
          streamVersion: 1,
          type: kind === "task" ? "agentControl.task.created" : "agentControl.worktree.reserved",
          occurredAt: at,
          commandId,
          causationEventId: null,
          correlationId: commandId,
          authority: "controller",
          ...storage,
        };
        return yield* kind === "task"
          ? decodeAgentControlVerificationStoredTaskEvent(raw)
          : decodeAgentControlVerificationStoredWorktreeEvent(raw);
      });

      for (const [kind, payload] of [
        ["task", taskPayload],
        ["worktree", worktreePayload],
      ] as const) {
        const legacy = encodeUnknownJson(payload);
        const canonical = canonicalJson(payload as unknown as JsonValue);
        yield* decode(kind, legacy);
        yield* decode(kind, canonical);
        const object = payload as unknown as Record<string, JsonValue>;
        const invalid = [
          `${canonical} `,
          `{"projectId":${encodeUnknownJson(projectId)},${canonical.slice(1)}`,
          canonicalJson({ ...object, unexpectedAuthority: "forbidden" }),
          encodeUnknownJson(Object.fromEntries(Object.entries(object).toReversed())),
        ];
        for (const source of invalid) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(decode(kind, source))));
        }
        assert.isTrue(Exit.isFailure(yield* Effect.exit(decode(kind, canonical, undefined, true))));
      }
    }),
  );
});
