import type { AgentControlTaskSourcePrecondition } from "@t3tools/contracts";

import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";

export const fingerprintAgentControlRunOnceSource = (
  source: AgentControlTaskSourcePrecondition,
): string => sha256Utf8(canonicalJson(source));
