import { IsoDateTime, NonNegativeInt, PositiveInt, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type {
  AgentControlRepositoryError,
  AgentControlTaskReconcileConflictError,
} from "../../Errors.ts";

export const AgentControlTaskReconcileStatus = Schema.Literals([
  "reconciling",
  "completed",
  "recovery-required",
]);
export type AgentControlTaskReconcileStatus = typeof AgentControlTaskReconcileStatus.Type;

export const AgentControlTaskReconcileState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  targetSequence: PositiveInt,
  lastCompletedSequence: NonNegativeInt,
  revision: PositiveInt,
  status: AgentControlTaskReconcileStatus,
  updatedAt: IsoDateTime,
});
export type AgentControlTaskReconcileState = typeof AgentControlTaskReconcileState.Type;

type ReconcileStateError = AgentControlRepositoryError | AgentControlTaskReconcileConflictError;

export interface AgentControlTaskReconcileStateRepositoryShape {
  readonly get: (
    projectId: ProjectId,
  ) => Effect.Effect<Option.Option<AgentControlTaskReconcileState>, AgentControlRepositoryError>;
  readonly begin: (
    projectId: ProjectId,
    targetSequence: number,
    updatedAt: string,
  ) => Effect.Effect<AgentControlTaskReconcileState, ReconcileStateError>;
  readonly markRecoveryRequired: (
    projectId: ProjectId,
    targetSequence: number,
    expectedRevision: number,
    updatedAt: string,
  ) => Effect.Effect<AgentControlTaskReconcileState, ReconcileStateError>;
  readonly complete: (
    projectId: ProjectId,
    targetSequence: number,
    expectedRevision: number,
    updatedAt: string,
  ) => Effect.Effect<AgentControlTaskReconcileState, ReconcileStateError>;
}

export class AgentControlTaskReconcileStateRepository extends Context.Service<
  AgentControlTaskReconcileStateRepository,
  AgentControlTaskReconcileStateRepositoryShape
>()(
  "t3/agentControl/task/Services/AgentControlTaskReconcileState/AgentControlTaskReconcileStateRepository",
) {}
