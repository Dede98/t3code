/**
 * Project-scoped Agent Control policy persistence.
 *
 * Policies are T3 Code state keyed by the canonical project id. Repository
 * commits are deliberately not an input to this service.
 *
 * @module AgentControlProjectPolicies
 */
import {
  AgentControlProjectPolicy,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { PersistenceSqlError } from "../Errors.ts";

export const AgentControlProjectPolicyRecord = Schema.Struct({
  projectId: ProjectId,
  policy: AgentControlProjectPolicy,
  revision: PositiveInt,
  updatedAt: IsoDateTime,
});
export type AgentControlProjectPolicyRecord = typeof AgentControlProjectPolicyRecord.Type;

export const SetAgentControlProjectPolicyInput = Schema.Struct({
  projectId: ProjectId,
  policy: AgentControlProjectPolicy,
  /** Revision zero means that no policy row is expected to exist yet. */
  expectedRevision: NonNegativeInt,
});
export type SetAgentControlProjectPolicyInput = typeof SetAgentControlProjectPolicyInput.Type;

export const ClearAgentControlProjectPolicyInput = Schema.Struct({
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
});
export type ClearAgentControlProjectPolicyInput = typeof ClearAgentControlProjectPolicyInput.Type;

export class AgentControlProjectPolicyValidationError extends Schema.TaggedErrorClass<AgentControlProjectPolicyValidationError>()(
  "AgentControlProjectPolicyValidationError",
  {
    projectId: Schema.String,
    operation: Schema.Literals(["setProjectPolicy", "clearProjectPolicy"]),
    issue: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Agent Control project policy validation failed for ${this.projectId}: ${this.issue}`;
  }
}

export class AgentControlProjectPolicyConflictError extends Schema.TaggedErrorClass<AgentControlProjectPolicyConflictError>()(
  "AgentControlProjectPolicyConflictError",
  {
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: Schema.NullOr(PositiveInt),
  },
) {
  override get message(): string {
    const actual = this.actualRevision === null ? "missing" : String(this.actualRevision);
    return `Agent Control project policy revision conflict for ${this.projectId}: expected ${this.expectedRevision}, found ${actual}`;
  }
}

export class AgentControlProjectPolicyProjectUnavailableError extends Schema.TaggedErrorClass<AgentControlProjectPolicyProjectUnavailableError>()(
  "AgentControlProjectPolicyProjectUnavailableError",
  {
    projectId: ProjectId,
    reason: Schema.Literals(["missing", "deleted"]),
  },
) {
  override get message(): string {
    return `Cannot persist an Agent Control policy for ${this.reason} project ${this.projectId}`;
  }
}

/**
 * A persisted row exists but cannot be trusted as an Agent Control policy.
 * Consumers must not fall back to a less restrictive policy after this error.
 */
export class AgentControlProjectPolicyCorruptError extends Schema.TaggedErrorClass<AgentControlProjectPolicyCorruptError>()(
  "AgentControlProjectPolicyCorruptError",
  {
    projectId: ProjectId,
    issue: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Persisted Agent Control project policy for ${this.projectId} is corrupt: ${this.issue}`;
  }
}

export type GetAgentControlProjectPolicyError =
  | PersistenceSqlError
  | AgentControlProjectPolicyCorruptError;

export type SetAgentControlProjectPolicyError =
  | PersistenceSqlError
  | AgentControlProjectPolicyValidationError
  | AgentControlProjectPolicyConflictError
  | AgentControlProjectPolicyProjectUnavailableError
  | AgentControlProjectPolicyCorruptError;

export type ClearAgentControlProjectPolicyError = SetAgentControlProjectPolicyError;

export type EnsureAgentControlProjectAvailableError =
  | PersistenceSqlError
  | AgentControlProjectPolicyProjectUnavailableError;

export interface AgentControlProjectPolicyRepositoryShape {
  readonly ensureProjectAvailable: (
    projectId: ProjectId,
  ) => Effect.Effect<void, EnsureAgentControlProjectAvailableError>;

  readonly getProjectPolicy: (
    projectId: ProjectId,
  ) => Effect.Effect<
    Option.Option<AgentControlProjectPolicyRecord>,
    GetAgentControlProjectPolicyError
  >;

  readonly setProjectPolicy: (
    input: SetAgentControlProjectPolicyInput,
  ) => Effect.Effect<AgentControlProjectPolicyRecord, SetAgentControlProjectPolicyError>;

  readonly clearProjectPolicy: (
    input: ClearAgentControlProjectPolicyInput,
  ) => Effect.Effect<void, ClearAgentControlProjectPolicyError>;

  /** Unconditional cleanup reserved for a committed project.deleted event. */
  readonly deleteProjectPolicy: (projectId: ProjectId) => Effect.Effect<void, PersistenceSqlError>;
}

export class AgentControlProjectPolicyRepository extends Context.Service<
  AgentControlProjectPolicyRepository,
  AgentControlProjectPolicyRepositoryShape
>()("t3/persistence/Services/AgentControlProjectPolicies/AgentControlProjectPolicyRepository") {}
