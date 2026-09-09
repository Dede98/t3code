import { IsoDateTime, ProjectId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class AgentControlArmedError extends Schema.TaggedError<AgentControlArmedError>()(
  "AgentControlArmedError",
  {
    projectId: ProjectId,
    reason: Schema.Literals([
      "authority-conflict",
      "identity-mismatch",
      "partial-replay",
      "projection-corrupt",
      "source-unavailable",
      "source-watermark-stale",
      "task-history-corrupt",
      "mode-superseded",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
    retryAt: Schema.optional(IsoDateTime),
  },
) {}
