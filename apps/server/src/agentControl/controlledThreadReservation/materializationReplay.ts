import {
  AgentControlProjectionCorruptError,
  type AgentControlThreadMaterializeCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { loadAuthoritativeControlledThreadReservation } from "./authoritative.ts";
import { AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR } from "./invariant.ts";
import { validateCanonicalControlledThreadSuccessors } from "./successorEvidence.ts";
import type { AgentControlControlledThreadReservationEventStoreShape } from "./Services/AgentControlControlledThreadReservationEventStore.ts";
import type { AgentControlControlledThreadReservationStateRepositoryShape } from "./Services/AgentControlControlledThreadReservationStateRepository.ts";

// Successor admission and materialization receipts belong to their stage's own
// coordinator. Replay its reservation against that validated command, without
// requiring the initial planning coordinator's separate receipt tables.
export const loadControlledThreadMaterializationReplay = Effect.fn(
  "loadControlledThreadMaterializationReplay",
)(function* (
  command: AgentControlThreadMaterializeCommand,
  commandFingerprint: string,
  events: Pick<AgentControlControlledThreadReservationEventStoreShape, "readStream">,
  states: Pick<AgentControlControlledThreadReservationStateRepositoryShape, "get">,
) {
  const state = yield* loadAuthoritativeControlledThreadReservation(
    command.controlledThreadReservationId,
    events,
    states,
  );
  const history = yield* events.readStream(command.controlledThreadReservationId, 0, 4);
  const current = Option.getOrNull(state);
  if (
    current === null ||
    current.status !== "bound" ||
    history[0] === undefined ||
    current.threadId !== command.threadId ||
    current.projectId !== command.projectId ||
    current.taskId !== command.taskId ||
    current.taskRevision !== command.taskRevision ||
    current.githubIntakeSequence !== command.githubIntakeSequence ||
    current.sourceIdentityFingerprint !== command.sourceIdentityFingerprint ||
    current.stageRunId !== command.stageRunId ||
    current.attemptId !== command.attemptId ||
    current.roleId !== command.roleId ||
    current.stageKind !== command.stageKind ||
    current.stageOrdinal !== command.stageOrdinal ||
    current.attemptOrdinal !== command.attemptOrdinal ||
    current.leaseId !== command.leaseId ||
    current.fenceToken !== command.fenceToken ||
    current.worktreeReservationId !== command.worktreeReservationId ||
    current.materializationCommandId !== command.commandId ||
    current.materializationCommandFingerprint !== commandFingerprint ||
    current.materializedAt !== command.createdAt ||
    !(yield* validateCanonicalControlledThreadSuccessors({
      prepareCommandId: history[0].commandId,
      state: current,
      history,
    }))
  ) {
    return yield* new AgentControlProjectionCorruptError({
      code: "projection-corrupt",
      projector: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
    });
  }
  return { currentState: current, history };
});
