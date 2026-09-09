import { AgentControlRunOnceId, AgentControlRunOnceStep, ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class AgentControlRunOnceError extends Schema.TaggedError<AgentControlRunOnceError>()(
  "AgentControlRunOnceError",
  {
    projectId: ProjectId,
    runId: Schema.NullOr(AgentControlRunOnceId),
    step: Schema.NullOr(AgentControlRunOnceStep),
    reason: Schema.Literals([
      "authority-conflict",
      "partial-replay",
      "identity-mismatch",
      "projection-corrupt",
      "source-unavailable",
      "source-watermark-stale",
      "task-history-corrupt",
      "downstream-rejected",
      "mode-superseded",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface RunOnceStepBindings {
  readonly taskId?: string | null;
  readonly stageRunId?: string | null;
  readonly leaseId?: string | null;
  readonly worktreeReservationId?: string | null;
  readonly controlledThreadReservationId?: string | null;
  readonly terminalTaskEventId?: string | null;
  readonly terminalTaskEventSequence?: number | null;
  readonly terminalTaskEventStreamVersion?: number | null;
  readonly modeEventId?: string | null;
  readonly modeEventSequence?: number | null;
  readonly modeEventStreamVersion?: number | null;
  readonly modeExpectedRevision?: number | null;
  readonly modeCommandFingerprint?: string | null;
  readonly modeEventPayloadBytes?: Uint8Array | null;
  readonly modeEventMetadataBytes?: Uint8Array | null;
}
