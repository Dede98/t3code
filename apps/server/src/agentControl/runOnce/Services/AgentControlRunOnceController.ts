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

export type AgentControlRunOncePublicationConsumerId = string;

export interface AgentControlRunOnceControllerShape {
  readonly recover: Effect.Effect<void, AgentControlRunOnceError>;
  readonly processProject: (projectId: ProjectId) => Effect.Effect<void, AgentControlRunOnceError>;
  readonly prepare: (
    activation: ReactorStartupActivation,
  ) => Effect.Effect<void, AgentControlRunOnceError, Scope.Scope>;
  /** Volatile, idempotent hints only. Meaningful delivery uses pull + acknowledge below. */
  readonly subscribePublicationWakeups: Effect.Effect<
    Stream.Stream<AgentControlRunOnceCommittedPublication>,
    never,
    Scope.Scope
  >;
  /** Reclaims only unacknowledged durable inbox entries after a consumer restart. */
  readonly recoverPublicationConsumer: (
    consumerId: AgentControlRunOncePublicationConsumerId,
  ) => Effect.Effect<void, AgentControlRunOnceError>;
  /** Claims durable, unacknowledged inbox entries for this runtime. */
  readonly pullPublications: (
    consumerId: AgentControlRunOncePublicationConsumerId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlRunOnceCommittedPublication>,
    AgentControlRunOnceError
  >;
  /** Durable semantic acknowledgement; acknowledged entries are never pulled again. */
  readonly acknowledgePublication: (
    consumerId: AgentControlRunOncePublicationConsumerId,
    publicationId: string,
  ) => Effect.Effect<void, AgentControlRunOnceError>;
  /** @deprecated Wakeup-only compatibility alias. */
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
