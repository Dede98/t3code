import type {
  AgentControlStageKind,
  AgentControlStageRunId,
  AgentControlStageRunState,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlStageRunEnumerationEntry =
  | { readonly _tag: "Valid"; readonly state: AgentControlStageRunState }
  | {
      readonly _tag: "Corrupt";
      readonly stageRunId: AgentControlStageRunId | null;
      readonly projectId: ProjectId | null;
      readonly taskId: AgentControlTaskId | null;
    };

export interface AgentControlStageRunSnapshotIdentity {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly stageKind: AgentControlStageKind;
  readonly stageOrdinal: number;
  readonly sourceIdentityFingerprint: string;
}

export interface AgentControlStageRunStateRepositoryShape {
  readonly get: (
    stageRunId: AgentControlStageRunId,
  ) => Effect.Effect<Option.Option<AgentControlStageRunState>, AgentControlRepositoryError>;
  readonly save: (
    state: AgentControlStageRunState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly findInitialForTask: (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) => Effect.Effect<Option.Option<AgentControlStageRunState>, AgentControlRepositoryError>;
  readonly findBySnapshot: (
    identity: AgentControlStageRunSnapshotIdentity,
  ) => Effect.Effect<Option.Option<AgentControlStageRunState>, AgentControlRepositoryError>;
  readonly listProject: (
    projectId: ProjectId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunEnumerationEntry>,
    AgentControlRepositoryError
  >;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlStageRunStateRepository extends Context.Service<
  AgentControlStageRunStateRepository,
  AgentControlStageRunStateRepositoryShape
>()("t3/agentControl/stageRun/Services/AgentControlStageRunStateRepository") {}
