import type {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationState,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlControlledThreadReservationEnumerationEntry =
  | {
      readonly _tag: "Valid";
      readonly state: AgentControlControlledThreadReservationState;
    }
  | {
      readonly _tag: "Corrupt";
      readonly controlledThreadReservationId: AgentControlControlledThreadReservationId | null;
      readonly projectId: ProjectId | null;
      readonly taskId: AgentControlTaskId | null;
    };

export interface AgentControlControlledThreadReservationSemanticPosition {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly roleId: AgentControlRoleId;
  readonly stageOrdinal: number;
  readonly attemptOrdinal: number;
}

export interface AgentControlControlledThreadReservationStateRepositoryShape {
  readonly get: (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
  ) => Effect.Effect<
    Option.Option<AgentControlControlledThreadReservationState>,
    AgentControlRepositoryError
  >;
  readonly save: (
    state: AgentControlControlledThreadReservationState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly listTask: (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationState>,
    AgentControlRepositoryError
  >;
  readonly listAll: Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationState>,
    AgentControlRepositoryError
  >;
  readonly findBySemanticPosition: (
    position: AgentControlControlledThreadReservationSemanticPosition,
  ) => Effect.Effect<
    Option.Option<AgentControlControlledThreadReservationState>,
    AgentControlRepositoryError
  >;
  readonly listProject: (
    projectId: ProjectId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEnumerationEntry>,
    AgentControlRepositoryError
  >;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlControlledThreadReservationStateRepository extends Context.Service<
  AgentControlControlledThreadReservationStateRepository,
  AgentControlControlledThreadReservationStateRepositoryShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservationStateRepository",
) {}
