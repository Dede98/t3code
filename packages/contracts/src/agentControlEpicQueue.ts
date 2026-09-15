import * as Schema from "effect/Schema";
import { CommandId, IsoDateTime, NonNegativeInt, PositiveInt, ProjectId } from "./baseSchemas.ts";
import { AgentControlEpicSource } from "./agentControlEpic.ts";
import { AgentControlEpicBlocker } from "./agentControlEpicRuntime.ts";

export const AgentControlEpicQueueEntry = Schema.Struct({
  entryId: Schema.String,
  source: AgentControlEpicSource,
  approvedAt: IsoDateTime,
  epicRunId: Schema.NullOr(Schema.String),
  status: Schema.Literals(["pending", "active", "merged"]),
  blockers: Schema.Array(AgentControlEpicBlocker),
});
export type AgentControlEpicQueueEntry = typeof AgentControlEpicQueueEntry.Type;
/** Array order is execution order; active and completed entries cannot be edited. */
export const AgentControlEpicQueue = Schema.Struct({
  projectId: ProjectId,
  revision: NonNegativeInt,
  entries: Schema.Array(AgentControlEpicQueueEntry),
  nextEntryId: Schema.NullOr(Schema.String),
  waitReason: Schema.NullOr(Schema.String),
  nextCheckAt: Schema.NullOr(IsoDateTime),
});
export type AgentControlEpicQueue = typeof AgentControlEpicQueue.Type;
export const AgentControlEpicQueueChangeInput = Schema.Struct({
  projectId: ProjectId,
  commandId: CommandId,
  expectedRevision: NonNegativeInt,
  action: Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("approve"),
      epicNumber: PositiveInt,
      expectedFingerprint: Schema.String,
    }),
    Schema.Struct({ kind: Schema.Literal("remove"), entryId: Schema.String }),
    Schema.Struct({ kind: Schema.Literal("reorder"), entryIds: Schema.Array(Schema.String) }),
  ]),
});
export type AgentControlEpicQueueChangeInput = typeof AgentControlEpicQueueChangeInput.Type;
export const AGENT_CONTROL_EPIC_QUEUE_RPC_METHODS = {
  change: "agentControlEpicQueue.change",
} as const;
