import type { AgentControlRunOnceId, AgentControlRunOnceStep, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type { ReactorStartupActivation } from "../../../reactorStartupActivation.ts";
import type { AgentControlRunOnceError } from "../model.ts";

export interface AgentControlRunOnceCommittedPublication {
  readonly publicationId: string;
  readonly runId: AgentControlRunOnceId;
  readonly projectId: ProjectId;
  readonly ordinal: number;
  readonly step: AgentControlRunOnceStep;
}

export interface AgentControlRunOnceControllerShape {
  readonly recover: Effect.Effect<void, AgentControlRunOnceError>;
  readonly processProject: (projectId: ProjectId) => Effect.Effect<void, AgentControlRunOnceError>;
  readonly prepare: (
    activation: ReactorStartupActivation,
  ) => Effect.Effect<void, never, Scope.Scope>;
  readonly subscribePublications: Effect.Effect<
    Stream.Stream<AgentControlRunOnceCommittedPublication>,
    never,
    Scope.Scope
  >;
}

export class AgentControlRunOnceController extends Context.Service<
  AgentControlRunOnceController,
  AgentControlRunOnceControllerShape
>()("t3/agentControl/runOnce/Services/AgentControlRunOnceController") {}
