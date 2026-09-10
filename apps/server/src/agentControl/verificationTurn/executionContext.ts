import type { AgentControlVerificationChecks, ThreadId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type { VerificationCheckCommandResult, VerificationCheckError } from "./checkEvidence.ts";
import * as Context from "effect/Context";

/** Set only inside ProviderService's fenced, admitted verification boundaries.
 * Authorizes the fixed check tool, never arbitrary commands or sandbox escapes.
 * Each turn must acquire its own authorization; it is not persisted on sessions.
 */
export const AgentControlVerificationExecution = Context.Reference<{
  readonly threadId: ThreadId;
  readonly cwd: string;
  readonly checks: AgentControlVerificationChecks;
  readonly runCheck: <E>(
    checkId: string,
    providerTurnId: string,
    execute: Effect.Effect<VerificationCheckCommandResult, E>,
  ) => Effect.Effect<VerificationCheckCommandResult, VerificationCheckError>;
} | null>("t3/agentControl/verificationTurn/Execution", { defaultValue: () => null });
