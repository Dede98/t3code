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

export const AgentControlRuntimeLayerLive = Layer.mergeAll(
  AgentControlEventInfrastructureLive,
  AgentControlProjectionLayerLive,
  AgentControlGithubProjectionLayerLive,
  AgentControlGithubIntakeLayerLive,
  AgentControlEngineLive.pipe(
    Layer.provide(
      Layer.merge(AgentControlEventInfrastructureLive, AgentControlProjectionLayerLive),
    ),
  ),
);
