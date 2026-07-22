import {
  AGENT_CONTROL_RPC_METHODS,
  AuthAccessWriteScope,
  AuthOrchestrationReadScope,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { RPC_REQUIRED_SCOPE } from "./ws.ts";

describe("Agent Control RPC registration", () => {
  it("registers every policy RPC in the group and authorization scope map", () => {
    const expectedScopes = new Map([
      [AGENT_CONTROL_RPC_METHODS.getPolicy, AuthOrchestrationReadScope],
      [AGENT_CONTROL_RPC_METHODS.preflightPolicy, AuthOrchestrationReadScope],
      [AGENT_CONTROL_RPC_METHODS.preflightRuntime, AuthOrchestrationReadScope],
      [AGENT_CONTROL_RPC_METHODS.setProjectPolicy, AuthAccessWriteScope],
      [AGENT_CONTROL_RPC_METHODS.clearProjectPolicy, AuthAccessWriteScope],
    ]);

    for (const [method, scope] of expectedScopes) {
      expect(WsRpcGroup.requests.has(method)).toBe(true);
      expect(RPC_REQUIRED_SCOPE.get(method)).toBe(scope);
    }
  });
});
