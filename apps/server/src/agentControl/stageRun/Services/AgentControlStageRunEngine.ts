import type {
  AgentControlStageRunCommand,
  AgentControlStageRunCommandResult,
  AgentControlStageRunEvent,
  AgentControlStageRunId,
  AgentControlStageRunRpcError,
  AgentControlStageRunState,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Stream from "effect/Stream";

export interface AgentControlStageRunDispatchCommit {
  readonly result: AgentControlStageRunCommandResult;
  readonly events: ReadonlyArray<AgentControlStageRunEvent>;
}

export type AgentControlStageRunDispatchOutcome =
  | ({ readonly _tag: "Accepted" } & AgentControlStageRunDispatchCommit)
  | { readonly _tag: "Rejected"; readonly error: AgentControlStageRunRpcError };

export interface AgentControlStageRunEngineShape {
  readonly get: (
    stageRunId: AgentControlStageRunId,
  ) => Effect.Effect<Option.Option<AgentControlStageRunState>, AgentControlStageRunRpcError>;
  /**
   * Must run inside AgentControlTaskConsumerGuard.useTaskConsumable. It writes
   * event, projection, and receipt but deliberately does not publish.
   */
  readonly dispatchPreparedController: (
    command: AgentControlStageRunCommand,
    commandFingerprint: string,
  ) => Effect.Effect<AgentControlStageRunDispatchOutcome, AgentControlStageRunRpcError>;
  readonly replayAccepted: (input: {
    readonly stageRunId: AgentControlStageRunId;
    readonly projectId: ProjectId;
    readonly resultStreamVersion: number;
    readonly resultSequence: number;
    readonly eventCreated: boolean;
  }) => Effect.Effect<AgentControlStageRunCommandResult, AgentControlStageRunRpcError>;
  readonly publishCommitted: (
    events: ReadonlyArray<AgentControlStageRunEvent>,
  ) => Effect.Effect<void>;
  readonly rebuild: Effect.Effect<void, AgentControlStageRunRpcError>;
  readonly streamDomainEvents: Stream.Stream<AgentControlStageRunEvent>;
}

export class AgentControlStageRunEngine extends Context.Service<
  AgentControlStageRunEngine,
  AgentControlStageRunEngineShape
>()("t3/agentControl/stageRun/Services/AgentControlStageRunEngine") {}
