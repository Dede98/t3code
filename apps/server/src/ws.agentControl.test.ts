import {
  AGENT_CONTROL_RPC_METHODS,
  AGENT_CONTROL_RUNTIME_RPC_METHODS,
  AGENT_CONTROL_GITHUB_RPC_METHODS,
  AuthAccessWriteScope,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { RPC_REQUIRED_SCOPE } from "./ws.ts";

describe("Agent Control RPC registration", () => {
  it("registers every Agent Control RPC in the group and authorization scope map", () => {
    const expectedScopes = new Map([
      [AGENT_CONTROL_RUNTIME_RPC_METHODS.getProjectState, AuthOrchestrationReadScope],
      [AGENT_CONTROL_RUNTIME_RPC_METHODS.setProjectMode, AuthAccessWriteScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.getTrackerConfig, AuthOrchestrationReadScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.setTrackerConfig, AuthAccessWriteScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.clearTrackerConfig, AuthAccessWriteScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.getObserveState, AuthOrchestrationReadScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.listObservedIssues, AuthOrchestrationReadScope],
      [AGENT_CONTROL_GITHUB_RPC_METHODS.pollOnce, AuthOrchestrationOperateScope],
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
