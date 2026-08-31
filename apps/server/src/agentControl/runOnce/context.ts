import type { AgentControlRunOnceId } from "@t3tools/contracts";
import * as Context from "effect/Context";

/** Fiber-local capability used only while an explicitly bound Run-Once seam executes. */
export const AgentControlRunOnceExecutionContext = Context.Reference<AgentControlRunOnceId | null>(
  "t3/agentControl/runOnce/AgentControlRunOnceExecutionContext",
  {
    defaultValue: () => null,
  },
);

/** Legacy test doubles may omit new internal seams; production callers fail closed. */
export const requireRunOnceMethod = <T>(method: T | undefined, name: string): T => {
  if (method === undefined) throw new Error(`Run-Once production seam unavailable: ${name}`);
  return method;
};
