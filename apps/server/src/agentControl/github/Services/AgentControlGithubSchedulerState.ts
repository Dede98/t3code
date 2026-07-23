import {
  AgentControlGithubCircuitState,
  AgentControlGithubPollIntervalSeconds,
  AgentControlGithubReactorReasonCode,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { AgentControlRepositoryError } from "../../Errors.ts";

/**
 * Durable operational state for the Observe scheduler. This is deliberately
 * separate from the event-sourced github-intake aggregate.
 */
export const AgentControlGithubSchedulerState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  generation: PositiveInt,
  configFingerprint: TrimmedNonEmptyString,
  pollIntervalSeconds: AgentControlGithubPollIntervalSeconds,
  lastGithubEventSequence: NonNegativeInt,
  activity: Schema.Literals(["active", "suspended"]),
  circuitState: AgentControlGithubCircuitState,
  consecutiveFailures: NonNegativeInt,
  lastAttemptAt: Schema.NullOr(IsoDateTime),
  nextAttemptAt: Schema.NullOr(IsoDateTime),
  cooldownUntil: Schema.NullOr(IsoDateTime),
  reasonCode: Schema.NullOr(AgentControlGithubReactorReasonCode),
  updatedAt: IsoDateTime,
});
export type AgentControlGithubSchedulerState = typeof AgentControlGithubSchedulerState.Type;

export interface AgentControlGithubSchedulerStateRepositoryShape {
  readonly get: (
    projectId: ProjectId,
  ) => Effect.Effect<Option.Option<AgentControlGithubSchedulerState>, AgentControlRepositoryError>;
  readonly save: (
    state: AgentControlGithubSchedulerState,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly delete: (projectId: ProjectId) => Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlGithubSchedulerStateRepository extends Context.Service<
  AgentControlGithubSchedulerStateRepository,
  AgentControlGithubSchedulerStateRepositoryShape
>()(
  "t3/agentControl/github/Services/AgentControlGithubSchedulerState/AgentControlGithubSchedulerStateRepository",
) {}
