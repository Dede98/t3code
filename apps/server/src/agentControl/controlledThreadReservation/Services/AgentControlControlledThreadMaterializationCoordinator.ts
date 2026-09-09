import {
  type AgentControlRunOnceId,
  AgentControlControlledThreadReservationId,
  CommandId,
  ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const AgentControlControlledThreadMaterializationCoordinatorReason = Schema.Literals([
  "validation",
  "command-identity-conflict",
  "project-unavailable",
  "project-mode-inactive",
  "task-unavailable",
  "source-snapshot-stale",
  "stage-run-unavailable",
  "lease-unavailable",
  "lease-expired",
  "lease-foreign-runtime",
  "worktree-unavailable",
  "reservation-missing",
  "reservation-not-prepared",
  "reservation-conflict",
  "runtime-policy-unavailable",
  "historical-evidence-corrupt",
  "internal-persistence-error",
]);
export type AgentControlControlledThreadMaterializationCoordinatorReason =
  typeof AgentControlControlledThreadMaterializationCoordinatorReason.Type;

export class AgentControlControlledThreadMaterializationCoordinatorError extends Schema.TaggedError<AgentControlControlledThreadMaterializationCoordinatorError>()(
  "AgentControlControlledThreadMaterializationCoordinatorError",
  {
    reason: AgentControlControlledThreadMaterializationCoordinatorReason,
    commandId: CommandId,
    projectId: ProjectId,
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/**
 * The complete server-internal input. In particular, no authority, derived
 * identity, policy selection, worktree path, binding, fence, or timestamp may
 * be supplied by a caller.
 */
export interface AgentControlControlledThreadMaterializeInitialInput {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
}

export interface AgentControlControlledThreadMaterializeInitialResult {
  readonly commandId: CommandId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly orchestrationResultSequence: number;
  readonly status: "bound";
  readonly replayed: boolean;
}

export interface AgentControlControlledThreadMaterializationCoordinatorShape {
  readonly materializeInitial: (
    input: AgentControlControlledThreadMaterializeInitialInput,
  ) => Effect.Effect<
    AgentControlControlledThreadMaterializeInitialResult,
    AgentControlControlledThreadMaterializationCoordinatorError
  >;
  readonly materializeInitialForRunOnce?: (
    runId: AgentControlRunOnceId,
    input: AgentControlControlledThreadMaterializeInitialInput,
  ) => Effect.Effect<
    AgentControlControlledThreadMaterializeInitialResult,
    AgentControlControlledThreadMaterializationCoordinatorError
  >;
}

export class AgentControlControlledThreadMaterializationCoordinator extends Context.Service<
  AgentControlControlledThreadMaterializationCoordinator,
  AgentControlControlledThreadMaterializationCoordinatorShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinator",
) {}
