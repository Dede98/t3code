import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface NodeSqlitePostCommitObservation {
  readonly boundary:
    | "agent-control-controlled-thread-materialization-coordinator"
    | "agent-control-controlled-thread-prepare-finalization"
    | "agent-control-initial-planning-stage-finalization"
    | "agent-control-implementation-admission"
    | "agent-control-implementation-materialization"
    | "agent-control-implementation-turn-acceptance"
    | "agent-control-implementation-stage-start";
}

export interface NodeSqliteTransactionHooksShape {
  readonly afterCommitBeforeReturn: (
    observation: NodeSqlitePostCommitObservation,
  ) => Effect.Effect<void>;
}

const noop = (_observation: NodeSqlitePostCommitObservation) => Effect.void;

/**
 * Production-bound test seam at the native SQLite COMMIT boundary. The
 * production default is a cached no-op.
 */
export const NodeSqliteTransactionHooks = Context.Reference<NodeSqliteTransactionHooksShape>(
  "t3/persistence/Services/NodeSqliteTransactionHooks",
  {
    defaultValue: () => ({
      afterCommitBeforeReturn: noop,
    }),
  },
);
