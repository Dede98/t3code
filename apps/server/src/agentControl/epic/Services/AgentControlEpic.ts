import type {
  AgentControlEpicControlInput,
  AgentControlEpicPreview,
  AgentControlEpicPreviewInput,
  AgentControlEpicRpcError,
  AgentControlEpicRuntimeView,
  AgentControlEpicStartInput,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

export interface AgentControlEpicShape {
  readonly subscribeChanges: Effect.Effect<Stream.Stream<ProjectId>, never, Scope.Scope>;
  readonly get: (
    projectId: ProjectId,
  ) => Effect.Effect<AgentControlEpicRuntimeView | null, AgentControlEpicRpcError>;
  readonly preview: (
    input: AgentControlEpicPreviewInput,
  ) => Effect.Effect<AgentControlEpicPreview, AgentControlEpicRpcError>;
  readonly start: (
    input: AgentControlEpicStartInput,
  ) => Effect.Effect<AgentControlEpicRuntimeView, AgentControlEpicRpcError>;
  readonly resume: (
    input: AgentControlEpicControlInput,
  ) => Effect.Effect<AgentControlEpicRuntimeView, AgentControlEpicRpcError>;
  readonly stop: (
    input: AgentControlEpicControlInput,
  ) => Effect.Effect<AgentControlEpicRuntimeView, AgentControlEpicRpcError>;
  readonly clear: (
    input: AgentControlEpicControlInput,
  ) => Effect.Effect<AgentControlEpicRuntimeView, AgentControlEpicRpcError>;
  readonly processProject: (projectId: ProjectId) => Effect.Effect<void, AgentControlEpicRpcError>;
}
export class AgentControlEpic extends Context.Service<AgentControlEpic, AgentControlEpicShape>()(
  "t3/agentControl/epic/Services/AgentControlEpic",
) {}
