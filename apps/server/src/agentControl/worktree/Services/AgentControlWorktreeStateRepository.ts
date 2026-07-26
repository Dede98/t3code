import type {
  AgentControlWorktreeReservationId,
  AgentControlWorktreeReservationState,
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlWorktreeEnumerationEntry =
  | { readonly _tag: "Valid"; readonly state: AgentControlWorktreeReservationState }
  | {
      readonly _tag: "Corrupt";
      readonly reservationId: AgentControlWorktreeReservationId | null;
      readonly projectId: ProjectId | null;
    };

export interface AgentControlWorktreeStateRepositoryShape {
  readonly get: (
    reservationId: AgentControlWorktreeReservationId,
  ) => Effect.Effect<
    Option.Option<AgentControlWorktreeReservationState>,
    AgentControlRepositoryError
  >;
  readonly getByStage: (input: {
    readonly projectId: ProjectId;
    readonly taskId: AgentControlTaskId;
    readonly stageRunId: AgentControlStageRunId;
    readonly attemptId: AgentControlAttemptId;
  }) => Effect.Effect<
    Option.Option<AgentControlWorktreeReservationState>,
    AgentControlRepositoryError
  >;
  readonly save: (
    state: AgentControlWorktreeReservationState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly listProject: (
    projectId: ProjectId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlWorktreeEnumerationEntry>,
    AgentControlRepositoryError
  >;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlWorktreeStateRepository extends Context.Service<
  AgentControlWorktreeStateRepository,
  AgentControlWorktreeStateRepositoryShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeStateRepository") {}
