import { AgentControlEpicRpcError, type ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

/** Production binds the Epic service here; legacy schedulers fail closed for selected Epics. */
export const AgentControlEpicProgress = Context.Reference<{
  readonly subscribeChanges?: Effect.Effect<Stream.Stream<ProjectId>, never, Scope.Scope>;
  readonly processProject: (projectId: ProjectId) => Effect.Effect<void, AgentControlEpicRpcError>;
}>("t3/agentControl/epic/Progress", {
  defaultValue: () => ({
    processProject: () =>
      Effect.fail(
        new AgentControlEpicRpcError({
          code: "epic-runtime-unavailable",
          message: "Epic execution is unavailable.",
        }),
      ),
  }),
});
