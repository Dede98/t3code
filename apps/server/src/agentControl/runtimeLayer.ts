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
import { layer as AgentControlTaskConsumerGuardLive } from "./task/Layers/AgentControlTaskConsumerGuard.ts";
import { layer as AgentControlStageRunEventStoreLive } from "./stageRun/Layers/AgentControlStageRunEventStore.ts";
import { layer as AgentControlStageRunStateRepositoryLive } from "./stageRun/Layers/AgentControlStageRunStateRepository.ts";
import { layer as AgentControlStageRunProjectionLive } from "./stageRun/Layers/AgentControlStageRunProjection.ts";
import { layer as AgentControlStageRunEngineLive } from "./stageRun/Layers/AgentControlStageRunEngine.ts";
import { layer as AgentControlStageRunLive } from "./stageRun/Layers/AgentControlStageRun.ts";
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
  AgentControlStageRunEventStoreLive,
  AgentControlStageRunStateRepositoryLive,
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

export const AgentControlTaskConsumerGuardLayerLive = AgentControlTaskConsumerGuardLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlStageRunProjectionLayerLive = AgentControlStageRunProjectionLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlStageRunEngineLayerLive = AgentControlStageRunEngineLive.pipe(
  Layer.provide(
    Layer.merge(AgentControlEventInfrastructureLive, AgentControlStageRunProjectionLayerLive),
  ),
);

export const AgentControlStageRunLayerLive = AgentControlStageRunLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentControlEventInfrastructureLive,
      AgentControlStageRunEngineLayerLive,
      AgentControlTaskConsumerGuardLayerLive,
    ),
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
  AgentControlTaskConsumerGuardLayerLive,
  AgentControlStageRunProjectionLayerLive,
  AgentControlStageRunEngineLayerLive,
  AgentControlStageRunLayerLive,
  AgentControlEngineLive.pipe(
    Layer.provide(
      Layer.merge(AgentControlEventInfrastructureLive, AgentControlProjectionLayerLive),
    ),
  ),
);
