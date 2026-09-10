import { createAgentControlEnvironmentAtoms } from "@t3tools/client-runtime/state/agent-control";

import { connectionAtomRuntime } from "../connection/runtime";

export const agentControlEnvironment = createAgentControlEnvironmentAtoms(connectionAtomRuntime);
