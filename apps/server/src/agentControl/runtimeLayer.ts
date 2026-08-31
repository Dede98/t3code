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
import { layer as AgentControlStageRunLeaseEventStoreLive } from "./stageRunLease/Layers/AgentControlStageRunLeaseEventStore.ts";
import { layer as AgentControlStageRunLeaseStateRepositoryLive } from "./stageRunLease/Layers/AgentControlStageRunLeaseStateRepository.ts";
import { layer as AgentControlStageRunLeaseProjectionLive } from "./stageRunLease/Layers/AgentControlStageRunLeaseProjection.ts";
import { layer as AgentControlStageRunLeaseEngineLive } from "./stageRunLease/Layers/AgentControlStageRunLeaseEngine.ts";
import { layer as AgentControlStageRunLeaseLive } from "./stageRunLease/Layers/AgentControlStageRunLease.ts";
import { layer as AgentControlWorktreeEventStoreLive } from "./worktree/Layers/AgentControlWorktreeEventStore.ts";
import { layer as AgentControlWorktreeStateRepositoryLive } from "./worktree/Layers/AgentControlWorktreeStateRepository.ts";
import { layer as AgentControlWorktreeProjectionLive } from "./worktree/Layers/AgentControlWorktreeProjection.ts";
import { layer as AgentControlWorktreeEngineLive } from "./worktree/Layers/AgentControlWorktreeEngine.ts";
import { layer as AgentControlWorktreeLive } from "./worktree/Layers/AgentControlWorktree.ts";
import { layer as AgentControlWorktreeControllerLive } from "./worktree/Layers/AgentControlWorktreeController.ts";
import { layer as AgentControlControlledThreadReservationEventStoreLive } from "./controlledThreadReservation/Layers/AgentControlControlledThreadReservationEventStore.ts";
import { layer as AgentControlControlledThreadReservationStateRepositoryLive } from "./controlledThreadReservation/Layers/AgentControlControlledThreadReservationStateRepository.ts";
import { layer as AgentControlControlledThreadReservationProjectionLive } from "./controlledThreadReservation/Layers/AgentControlControlledThreadReservationProjection.ts";
import { layer as AgentControlControlledThreadReservationEngineLive } from "./controlledThreadReservation/Layers/AgentControlControlledThreadReservationEngine.ts";
import { layer as AgentControlControlledThreadReservationLive } from "./controlledThreadReservation/Layers/AgentControlControlledThreadReservation.ts";
import { AgentControlRunOnceControllerLive } from "./runOnce/Layers/AgentControlRunOnceController.ts";
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
  AgentControlStageRunLeaseEventStoreLive,
  AgentControlStageRunLeaseStateRepositoryLive,
  AgentControlWorktreeEventStoreLive,
  AgentControlWorktreeStateRepositoryLive,
  AgentControlControlledThreadReservationEventStoreLive,
  AgentControlControlledThreadReservationStateRepositoryLive,
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

export const AgentControlStageRunLeaseProjectionLayerLive =
  AgentControlStageRunLeaseProjectionLive.pipe(Layer.provide(AgentControlEventInfrastructureLive));

export const AgentControlStageRunLeaseEngineLayerLive = AgentControlStageRunLeaseEngineLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentControlEventInfrastructureLive,
      AgentControlStageRunLeaseProjectionLayerLive,
      AgentControlTaskConsumerGuardLayerLive,
    ),
  ),
);

export const AgentControlStageRunLeaseLayerLive = AgentControlStageRunLeaseLive.pipe(
  Layer.provide(
    Layer.mergeAll(AgentControlEventInfrastructureLive, AgentControlStageRunLeaseEngineLayerLive),
  ),
);

export const AgentControlWorktreeProjectionLayerLive = AgentControlWorktreeProjectionLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlWorktreeEngineLayerLive = AgentControlWorktreeEngineLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentControlEventInfrastructureLive,
      AgentControlWorktreeProjectionLayerLive,
      AgentControlTaskConsumerGuardLayerLive,
      AgentControlStageRunLeaseEngineLayerLive,
    ),
  ),
);

export const AgentControlWorktreeLayerLive = AgentControlWorktreeLive.pipe(
  Layer.provide(AgentControlEventInfrastructureLive),
);

export const AgentControlWorktreeControllerLayerLive = AgentControlWorktreeControllerLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentControlEventInfrastructureLive,
      AgentControlWorktreeEngineLayerLive,
      AgentControlTaskConsumerGuardLayerLive,
      AgentControlStageRunLeaseEngineLayerLive,
    ),
  ),
);

export const AgentControlControlledThreadReservationProjectionLayerLive =
  AgentControlControlledThreadReservationProjectionLive.pipe(
    Layer.provide(AgentControlEventInfrastructureLive),
  );

export const AgentControlControlledThreadReservationEngineLayerLive =
  AgentControlControlledThreadReservationEngineLive.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentControlEventInfrastructureLive,
        AgentControlControlledThreadReservationProjectionLayerLive,
        AgentControlTaskConsumerGuardLayerLive,
        AgentControlStageRunLeaseEngineLayerLive,
        AgentControlWorktreeLayerLive,
        AgentControlWorktreeEngineLayerLive,
      ),
    ),
  );

export const AgentControlControlledThreadReservationLayerLive =
  AgentControlControlledThreadReservationLive.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentControlEventInfrastructureLive,
        AgentControlControlledThreadReservationEngineLayerLive,
        AgentControlTaskConsumerGuardLayerLive,
        AgentControlStageRunLeaseEngineLayerLive,
        AgentControlWorktreeLayerLive,
        AgentControlWorktreeEngineLayerLive,
        AgentControlWorktreeControllerLayerLive,
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
  AgentControlStageRunLeaseProjectionLayerLive,
  AgentControlStageRunLeaseEngineLayerLive,
  AgentControlStageRunLeaseLayerLive,
  AgentControlWorktreeProjectionLayerLive,
  AgentControlWorktreeEngineLayerLive,
  AgentControlWorktreeLayerLive,
  AgentControlControlledThreadReservationProjectionLayerLive,
  AgentControlControlledThreadReservationEngineLayerLive,
  AgentControlEngineLive.pipe(
    Layer.provide(
      Layer.merge(AgentControlEventInfrastructureLive, AgentControlProjectionLayerLive),
    ),
  ),
);

/**
 * Production run-once controller with every runtime-owned dependency supplied.
 * The server boundary supplies the controlled-thread activation service because
 * that service also depends on orchestration/provider infrastructure.
 */
export const AgentControlRunOnceControllerLayerLive = AgentControlRunOnceControllerLive.pipe(
  Layer.provideMerge(AgentControlRuntimeLayerLive),
  Layer.provideMerge(AgentControlWorktreeControllerLayerLive),
);
