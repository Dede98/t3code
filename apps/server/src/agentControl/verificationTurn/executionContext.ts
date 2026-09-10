import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";

/** Set only inside ProviderService's fenced, admitted verification boundaries.
 * Authorizes the fixed check tool, never arbitrary commands or sandbox escapes.
 * Each turn must acquire its own authorization; it is not persisted on sessions.
 */
export const AgentControlVerificationExecution = Context.Reference<{
  readonly threadId: ThreadId;
  readonly cwd: string;
} | null>("t3/agentControl/verificationTurn/Execution", { defaultValue: () => null });
