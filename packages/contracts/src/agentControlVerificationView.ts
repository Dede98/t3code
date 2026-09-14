import * as Schema from "effect/Schema";
import { IsoDateTime } from "./baseSchemas.ts";

export const AgentControlRunOnceCheckView = Schema.Struct({
  id: Schema.String,
  command: Schema.String,
  args: Schema.Array(Schema.String),
  cwd: Schema.String,
  required: Schema.Boolean,
  status: Schema.Literals(["passed", "failed", "unavailable", "stale", "missing", "running"]),
  exitCode: Schema.NullOr(Schema.Int),
  output: Schema.NullOr(Schema.String),
  completedAt: Schema.NullOr(IsoDateTime),
});
