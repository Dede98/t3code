import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
  ThreadId,
  type AgentControlControlledThreadReservationCommand,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const command = {
  type: "agentControl.controlledThreadReservation.transition",
  commandId: CommandId.make("controlled-thread-rejected-replay"),
  authority: "controller",
  controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
    "controlled-thread-reservation-rejected-replay",
  ),
  threadId: ThreadId.make("t3-auto-reserved-thread-rejected-replay"),
  projectId: ProjectId.make("controlled-thread-rejected-project"),
  taskId: AgentControlTaskId.make("controlled-thread-rejected-task"),
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: AgentControlStageRunId.make("controlled-thread-rejected-stage"),
  attemptId: AgentControlAttemptId.make("controlled-thread-rejected-attempt"),
  roleId: AgentControlRoleId.make("planning"),
  stageKind: "planning",
  stageOrdinal: 1,
  attemptOrdinal: 1,
  leaseId: AgentControlStageRunLeaseId.make("controlled-thread-rejected-lease"),
  fenceToken: 1,
  worktreeReservationId: AgentControlWorktreeReservationId.make(
    "controlled-thread-rejected-worktree",
  ),
  expectedRevision: 0,
  targetStatus: "materializing",
} as const satisfies AgentControlControlledThreadReservationCommand;
const requestFingerprint = "b".repeat(64);

layer("Controlled Thread rejected receipt replay", (it) => {
  it.effect("replays the identical rejection and binds every internal command coordinate", () =>
    Effect.gen(function* () {
      const engine = yield* AgentControlControlledThreadReservationEngine;
      const first = yield* engine.dispatchPreparedController(command, requestFingerprint);
      const second = yield* engine.dispatchPreparedController(command, requestFingerprint);
      assert.equal(first._tag, "Rejected");
      assert.equal(second._tag, "Rejected");
      if (first._tag === "Rejected" && second._tag === "Rejected") {
        assert.equal(first.error.code, "state-not-available");
        assert.equal(second.error.code, first.error.code);
      }
      const crossCommandReplay = yield* Effect.flip(
        engine.replayReceiptFirst({
          commandId: command.commandId,
          projectId: command.projectId,
          taskId: command.taskId,
          commandFingerprint: requestFingerprint,
        }),
      );
      assert.equal(crossCommandReplay.code, "command-identity-mismatch");

      const mutations: ReadonlyArray<AgentControlControlledThreadReservationCommand> = [
        {
          ...command,
          controlledThreadReservationId:
            AgentControlControlledThreadReservationId.make("other-reservation"),
        },
        { ...command, threadId: ThreadId.make("t3-auto-reserved-thread-other") },
        { ...command, stageRunId: AgentControlStageRunId.make("other-stage") },
        { ...command, attemptId: AgentControlAttemptId.make("other-attempt") },
        { ...command, roleId: AgentControlRoleId.make("other-role") },
        { ...command, stageOrdinal: 2 },
        { ...command, attemptOrdinal: 2 },
        { ...command, taskRevision: 2 },
        { ...command, githubIntakeSequence: 2 },
        { ...command, sourceIdentityFingerprint: "c".repeat(64) },
        { ...command, leaseId: AgentControlStageRunLeaseId.make("other-lease") },
        { ...command, fenceToken: 2 },
        {
          ...command,
          worktreeReservationId: AgentControlWorktreeReservationId.make("other-worktree"),
        },
        {
          ...command,
          type: "agentControl.controlledThreadReservation.prepare",
        },
        { ...command, authority: "system" },
      ];
      for (const mutated of mutations) {
        const failure = yield* Effect.flip(
          engine.dispatchPreparedController(mutated, requestFingerprint),
        );
        assert.equal(failure.code, "command-identity-mismatch");
      }

      const requestMismatch = yield* Effect.flip(
        engine.dispatchPreparedController(command, "d".repeat(64)),
      );
      assert.equal(requestMismatch.code, "command-identity-mismatch");

      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, result_sequence AS "resultSequence",
            result_stream_version AS "resultStreamVersion",
            event_created AS "eventCreated", error_code AS "errorCode"
          FROM agent_control_command_receipts
          WHERE command_id = ${command.commandId}
        `,
        [
          {
            status: "rejected",
            resultSequence: 0,
            resultStreamVersion: 0,
            eventCreated: 0,
            errorCode: "state-not-available",
          },
        ],
      );
    }),
  );
});
