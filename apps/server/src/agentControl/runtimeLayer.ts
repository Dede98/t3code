import * as Layer from "effect/Layer";

import { AgentControlCommandReceiptRepositoryLive } from "../persistence/Layers/AgentControlCommandReceipts.ts";
import { AgentControlEventStoreLive } from "../persistence/Layers/AgentControlEventStore.ts";
import { AgentControlProjectAvailabilityLive } from "../persistence/Layers/AgentControlProjectAvailability.ts";
import {
  AgentControlProjectionStateRepositoryLive,
  AgentControlProjectStateRepositoryLive,
} from "../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlEngineLive } from "./Layers/AgentControlEngine.ts";
import { AgentControlProjectionLive } from "./Layers/AgentControlProjection.ts";

export const AgentControlEventInfrastructureLive = Layer.mergeAll(
  AgentControlEventStoreLive,
  AgentControlCommandReceiptRepositoryLive,
  AgentControlProjectAvailabilityLive,
  AgentControlProjectStateRepositoryLive,
  AgentControlProjectionStateRepositoryLive,
);

export const AgentControlProjectionLayerLive = AgentControlProjectionLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlRuntimeLayerLive = Layer.mergeAll(
  AgentControlEventInfrastructureLive,
  AgentControlProjectionLayerLive,
  AgentControlEngineLive.pipe(
    Layer.provide(
      Layer.merge(AgentControlEventInfrastructureLive, AgentControlProjectionLayerLive),
    ),
  ),
);
