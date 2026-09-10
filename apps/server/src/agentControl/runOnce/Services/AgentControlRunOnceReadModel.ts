import {
  AgentControlInternalPersistenceError,
  type AgentControlRunOnceSnapshot,
  type AgentControlRunOnceSnapshotInput,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

interface ReadModel {
  readonly getSnapshot: (
    input: AgentControlRunOnceSnapshotInput,
  ) => Effect.Effect<AgentControlRunOnceSnapshot, AgentControlInternalPersistenceError>;
  readonly subscribe: (
    input: AgentControlRunOnceSnapshotInput,
  ) => Effect.Effect<
    Stream.Stream<AgentControlRunOnceSnapshot, AgentControlInternalPersistenceError>,
    never,
    Scope.Scope
  >;
}

/** Older transport harnesses fail closed; production supplies the durable reader. */
export const AgentControlRunOnceReadModel = Context.Reference<ReadModel>(
  "t3/agentControl/runOnce/ReadModel",
  {
    defaultValue: () => ({
      getSnapshot: () =>
        Effect.fail(
          new AgentControlInternalPersistenceError({ code: "internal-persistence-error" }),
        ),
      subscribe: () =>
        Effect.succeed(
          Stream.fail(
            new AgentControlInternalPersistenceError({ code: "internal-persistence-error" }),
          ),
        ),
    }),
  },
);
