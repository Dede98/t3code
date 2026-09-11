import { createAgentControlSetupEnvironmentAtoms } from "@t3tools/client-runtime/state/agent-control-setup";

import { connectionAtomRuntime } from "../connection/runtime";

export const agentControlSetupEnvironment =
  createAgentControlSetupEnvironmentAtoms(connectionAtomRuntime);
