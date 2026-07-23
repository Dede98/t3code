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
import { layer as AgentControlGithubEventStoreLive } from "./github/Layers/AgentControlGithubEventStore.ts";
import { layer as AgentControlGithubStateRepositoryLive } from "./github/Layers/AgentControlGithubStateRepository.ts";
import { layer as AgentControlGithubProjectionLive } from "./github/Layers/AgentControlGithubProjection.ts";
import { layer as GithubIssueTrackerClientLive } from "./github/Layers/GithubIssueTrackerClient.ts";
import { layer as AgentControlGithubIntakeLive } from "./github/Layers/AgentControlGithubIntake.ts";
import { layer as AgentControlGithubSchedulerStateLive } from "./github/Layers/AgentControlGithubSchedulerState.ts";
import { layer as AgentControlTaskEventStoreLive } from "./task/Layers/AgentControlTaskEventStore.ts";
import { layer as AgentControlTaskStateRepositoryLive } from "./task/Layers/AgentControlTaskStateRepository.ts";
import { layer as AgentControlTaskReconcileStateRepositoryLive } from "./task/Layers/AgentControlTaskReconcileState.ts";
import { layer as AgentControlTaskProjectionLive } from "./task/Layers/AgentControlTaskProjection.ts";
import { layer as AgentControlTaskEngineLive } from "./task/Layers/AgentControlTaskEngine.ts";
import { layer as AgentControlTaskIntakeLive } from "./task/Layers/AgentControlTaskIntake.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";

export const AgentControlEventInfrastructureLive = Layer.mergeAll(
  AgentControlEventStoreLive,
  AgentControlCommandReceiptRepositoryLive,
  AgentControlProjectAvailabilityLive,
  AgentControlProjectStateRepositoryLive,
  AgentControlProjectionStateRepositoryLive,
  AgentControlGithubEventStoreLive,
  AgentControlGithubStateRepositoryLive,
  AgentControlGithubSchedulerStateLive,
  AgentControlTaskEventStoreLive,
  AgentControlTaskStateRepositoryLive,
  AgentControlTaskReconcileStateRepositoryLive,
);

export const AgentControlGithubProjectionLayerLive = AgentControlGithubProjectionLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlProjectionLayerLive = AgentControlProjectionLive.pipe(
  Layer.provide(
    Layer.merge(AgentControlEventInfrastructureLive, AgentControlGithubProjectionLayerLive),
  ),
);

export const AgentControlGithubIntakeLayerLive = AgentControlGithubIntakeLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentControlEventInfrastructureLive,
      AgentControlGithubProjectionLayerLive,
      GithubIssueTrackerClientLive,
    ),
  ),
  Layer.provide(GitHubCli.layer),
  Layer.provide(VcsProcess.layer),
  Layer.provide(RepositoryIdentityResolver.layer),
);

export const AgentControlTaskProjectionLayerLive = AgentControlTaskProjectionLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlTaskEngineLayerLive = AgentControlTaskEngineLive.pipe(
  Layer.provide(
    Layer.merge(AgentControlEventInfrastructureLive, AgentControlTaskProjectionLayerLive),
  ),
);

export const AgentControlTaskIntakeLayerLive = AgentControlTaskIntakeLive.pipe(
  Layer.provide(
    Layer.mergeAll(AgentControlEventInfrastructureLive, AgentControlTaskEngineLayerLive),
  ),
);

export const AgentControlRuntimeLayerLive = Layer.mergeAll(
  AgentControlEventInfrastructureLive,
  AgentControlProjectionLayerLive,
  AgentControlGithubProjectionLayerLive,
  AgentControlGithubIntakeLayerLive,
  AgentControlTaskProjectionLayerLive,
  AgentControlTaskEngineLayerLive,
  AgentControlTaskIntakeLayerLive,
  AgentControlEngineLive.pipe(
    Layer.provide(
      Layer.merge(AgentControlEventInfrastructureLive, AgentControlProjectionLayerLive),
    ),
  ),
);
