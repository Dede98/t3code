/**
 * Schema-only contracts for the Agent Control runtime controller.
 *
 * This protocol deliberately does not extend the manual orchestration event
 * stream. Agent Control has its own lifecycle, command authority, recovery
 * boundary, and future project/task/run/GitHub aggregates. Keeping that
 * boundary explicit also prevents controller modes from becoming new manual
 * orchestration aggregate kinds.
 *
 * @module agentControlRuntime
 */
import * as Schema from "effect/Schema";

import {
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
} from "./baseSchemas.ts";

export const AGENT_CONTROL_RUNTIME_RPC_METHODS = {
  getProjectState: "agentControl.getProjectState",
  setProjectMode: "agentControl.setProjectMode",
} as const;

export const AGENT_CONTROL_PROJECT_MODES = [
  "manual",
  "observe",
  "run-once",
  "armed",
  "paused",
] as const;

export const AgentControlProjectMode = Schema.Literals(AGENT_CONTROL_PROJECT_MODES);
export type AgentControlProjectMode = typeof AgentControlProjectMode.Type;

export const AgentControlProjectState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  mode: AgentControlProjectMode,
  pausedFromMode: Schema.NullOr(AgentControlProjectMode),
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
  updatedAt: Schema.NullOr(IsoDateTime),
});
export type AgentControlProjectState = typeof AgentControlProjectState.Type;

export const AgentControlGetProjectStateInput = Schema.Struct({
  projectId: ProjectId,
});
export type AgentControlGetProjectStateInput = typeof AgentControlGetProjectStateInput.Type;

export const AgentControlSetProjectModeInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
  mode: AgentControlProjectMode,
});
export type AgentControlSetProjectModeInput = typeof AgentControlSetProjectModeInput.Type;

export const AgentControlSetProjectModeCommand = Schema.Struct({
  type: Schema.Literal("agentControl.project.mode.set"),
  commandId: CommandId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
  mode: AgentControlProjectMode,
});
export type AgentControlSetProjectModeCommand = typeof AgentControlSetProjectModeCommand.Type;

export const AgentControlProjectModeChangedPayload = Schema.Struct({
  projectId: ProjectId,
  previousMode: AgentControlProjectMode,
  mode: AgentControlProjectMode,
  previousPausedFromMode: Schema.NullOr(AgentControlProjectMode),
  pausedFromMode: Schema.NullOr(AgentControlProjectMode),
  changedAt: IsoDateTime,
});
export type AgentControlProjectModeChangedPayload =
  typeof AgentControlProjectModeChangedPayload.Type;

/** Event actors are observable metadata, never client-selectable command input. */
export const AgentControlEventAuthority = Schema.Literals(["human", "controller", "system"]);
export type AgentControlEventAuthority = typeof AgentControlEventAuthority.Type;

export const AgentControlEventMetadata = Schema.Struct({
  schemaVersion: Schema.Literal(1),
});
export type AgentControlEventMetadata = typeof AgentControlEventMetadata.Type;

export const AgentControlProjectModeChangedEventDraft = Schema.Struct({
  eventId: EventId,
  type: Schema.Literal("agentControl.project.mode.changed"),
  aggregateKind: Schema.Literal("project-controller"),
  aggregateId: ProjectId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: AgentControlEventAuthority,
  payload: AgentControlProjectModeChangedPayload,
  metadata: AgentControlEventMetadata,
});
export type AgentControlProjectModeChangedEventDraft =
  typeof AgentControlProjectModeChangedEventDraft.Type;

export const AgentControlProjectModeChangedEvent = Schema.Struct({
  ...AgentControlProjectModeChangedEventDraft.fields,
  streamVersion: PositiveInt,
  sequence: PositiveInt,
});
export type AgentControlProjectModeChangedEvent = typeof AgentControlProjectModeChangedEvent.Type;

export const AgentControlEvent = AgentControlProjectModeChangedEvent;
export type AgentControlEvent = typeof AgentControlEvent.Type;

export const AgentControlSetProjectModeResult = Schema.Struct({
  state: AgentControlProjectState,
  resultSequence: NonNegativeInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlSetProjectModeResult = typeof AgentControlSetProjectModeResult.Type;

export const AgentControlRejectedCommandErrorCode = Schema.Literals([
  "validation",
  "project-missing",
  "project-deleted",
  "revision-conflict",
  "transition-not-allowed",
  "mode-not-available",
]);
export type AgentControlRejectedCommandErrorCode = typeof AgentControlRejectedCommandErrorCode.Type;

export class AgentControlRuntimeValidationError extends Schema.TaggedErrorClass<AgentControlRuntimeValidationError>()(
  "AgentControlRuntimeValidationError",
  {
    code: Schema.Literal("validation"),
    operation: Schema.Literals(["get-project-state", "set-project-mode"]),
  },
) {}

export class AgentControlProjectMissingError extends Schema.TaggedErrorClass<AgentControlProjectMissingError>()(
  "AgentControlProjectMissingError",
  {
    code: Schema.Literal("project-missing"),
    projectId: ProjectId,
  },
) {}

export class AgentControlProjectDeletedError extends Schema.TaggedErrorClass<AgentControlProjectDeletedError>()(
  "AgentControlProjectDeletedError",
  {
    code: Schema.Literal("project-deleted"),
    projectId: ProjectId,
  },
) {}

export class AgentControlProjectRevisionConflictError extends Schema.TaggedErrorClass<AgentControlProjectRevisionConflictError>()(
  "AgentControlProjectRevisionConflictError",
  {
    code: Schema.Literal("revision-conflict"),
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: NonNegativeInt,
  },
) {}

export class AgentControlTransitionNotAllowedError extends Schema.TaggedErrorClass<AgentControlTransitionNotAllowedError>()(
  "AgentControlTransitionNotAllowedError",
  {
    code: Schema.Literal("transition-not-allowed"),
    projectId: ProjectId,
    fromMode: AgentControlProjectMode,
    toMode: AgentControlProjectMode,
  },
) {}

export class AgentControlModeNotAvailableError extends Schema.TaggedErrorClass<AgentControlModeNotAvailableError>()(
  "AgentControlModeNotAvailableError",
  {
    code: Schema.Literal("mode-not-available"),
    projectId: ProjectId,
    mode: Schema.Literals(["run-once", "armed"]),
  },
) {}

export class AgentControlCommandPreviouslyRejectedError extends Schema.TaggedErrorClass<AgentControlCommandPreviouslyRejectedError>()(
  "AgentControlCommandPreviouslyRejectedError",
  {
    code: Schema.Literal("command-previously-rejected"),
    commandId: CommandId,
    originalErrorCode: AgentControlRejectedCommandErrorCode,
  },
) {}

export class AgentControlCommandIdentityMismatchError extends Schema.TaggedErrorClass<AgentControlCommandIdentityMismatchError>()(
  "AgentControlCommandIdentityMismatchError",
  {
    code: Schema.Literal("command-identity-mismatch"),
    commandId: CommandId,
  },
) {}

export class AgentControlEventDecodeFailedError extends Schema.TaggedErrorClass<AgentControlEventDecodeFailedError>()(
  "AgentControlEventDecodeFailedError",
  {
    code: Schema.Literal("event-decode-failed"),
    operation: Schema.Literals(["stream-replay", "global-replay", "append"]),
  },
) {}

export class AgentControlProjectionCorruptError extends Schema.TaggedErrorClass<AgentControlProjectionCorruptError>()(
  "AgentControlProjectionCorruptError",
  {
    code: Schema.Literal("projection-corrupt"),
    projector: Schema.String,
  },
) {}

export class AgentControlInternalPersistenceError extends Schema.TaggedErrorClass<AgentControlInternalPersistenceError>()(
  "AgentControlInternalPersistenceError",
  {
    code: Schema.Literal("internal-persistence-error"),
  },
) {}

export const AgentControlRuntimeRpcError = Schema.Union([
  AgentControlRuntimeValidationError,
  AgentControlProjectMissingError,
  AgentControlProjectDeletedError,
  AgentControlProjectRevisionConflictError,
  AgentControlTransitionNotAllowedError,
  AgentControlModeNotAvailableError,
  AgentControlCommandPreviouslyRejectedError,
  AgentControlCommandIdentityMismatchError,
  AgentControlEventDecodeFailedError,
  AgentControlProjectionCorruptError,
  AgentControlInternalPersistenceError,
]);
export type AgentControlRuntimeRpcError = typeof AgentControlRuntimeRpcError.Type;
