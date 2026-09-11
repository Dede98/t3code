import { describe, expect, it, vi } from "@effect/vitest";
import {
  AuthAccessWriteScope,
  AuthOrchestrationOperateScope,
  ProjectId,
  ProviderInstanceId,
  type AgentControlGithubIntakeState,
  type AgentControlPolicyStateResult,
  type AgentControlVerificationCheck,
  type AuthSessionState,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  agentControlSetupPermissionBlocker,
  createAgentControlSetupController,
  type AgentControlSetupApi,
} from "./agentControlSetup.ts";

const projectId = ProjectId.make("fresh-project");
const check: AgentControlVerificationCheck = {
  id: "tests",
  command: "node",
  args: ["--test", "path with spaces.test.js", "", "$(literal)"],
  cwd: ".",
  required: true,
  timeoutMs: 30000,
  allowTemporaryFiles: false,
  resultFormat: "node-test",
};
const initialGithub: AgentControlGithubIntakeState = {
  schemaVersion: 1,
  projectId,
  revision: 0,
  sequence: 0,
  updatedAt: null,
  config: null,
  cursor: null,
  pollStatus: { status: "disabled", attemptedAt: null, completedAt: null, errorCode: null },
};
const initialPolicy: AgentControlPolicyStateResult = {
  appPolicy: null,
  projectPolicy: null,
  preflight: { ok: true, roles: [] },
};
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { resolve, promise };
};
function harness() {
  let policy = initialPolicy;
  let github = initialGithub;
  let connected = true;
  let writeBlocker: string | null = null;
  let importBlocker: string | null = null;
  const api: AgentControlSetupApi = {
    projectId,
    getPolicy: vi.fn(async () => policy),
    getGithub: vi.fn(async () => github),
    setPolicy: vi.fn(async (input) => {
      if (input.expectedRevision !== (policy.projectPolicy?.revision ?? 0))
        throw { code: "revision-conflict" };
      policy = {
        ...policy,
        projectPolicy: {
          projectId,
          policy: input.policy,
          revision: input.expectedRevision + 1,
          updatedAt: "2026-09-11T12:00:00.000Z",
        },
      };
      return policy;
    }),
    setGithub: vi.fn(async (input) => {
      github = {
        ...github,
        revision: github.revision + 1,
        sequence: github.sequence + 1,
        config: {
          schemaVersion: 1,
          projectId,
          revision: github.revision + 1,
          sequence: github.sequence + 1,
          updatedAt: "2026-09-11T12:00:00.000Z",
          repository: { repositoryNodeId: "repo-id", nameWithOwner: "owner/test" },
          settings: {
            trackerKind: "github",
            readyLabel: input.readyLabel,
            pausedLabel: input.pausedLabel,
            trustedLogins: input.trustedLogins,
            pollIntervalSeconds: input.pollIntervalSeconds,
          },
        },
      };
      return { state: github, resultSequence: github.sequence, eventCreated: true };
    }),
    clearGithub: vi.fn(async () => {
      github = { ...github, config: null, revision: github.revision + 1 };
      return { state: github, resultSequence: github.sequence, eventCreated: true };
    }),
    pollOnce: vi.fn(async () => {
      github = {
        ...github,
        sequence: github.sequence + 1,
        revision: github.revision + 1,
        pollStatus: {
          status: "success",
          attemptedAt: "2026-09-11T12:00:00.000Z",
          completedAt: "2026-09-11T12:00:00.000Z",
          errorCode: null,
          issueCount: 1,
        },
      };
      return { state: github, resultSequence: github.sequence, eventCreated: true };
    }),
    preflight: vi.fn(async () => ({
      ok: true,
      projectId,
      roles: [],
      staticPreflight: { ok: true as const, roles: [] },
    })),
    canWrite: () => writeBlocker,
    canImport: () => importBlocker,
    isCurrent: () => connected,
    onSaved: vi.fn(),
  };
  const controller = createAgentControlSetupController(api);
  return {
    api,
    controller,
    setConnected: (value: boolean) => {
      connected = value;
    },
    setWriteBlocker: (value: string | null) => {
      writeBlocker = value;
    },
    setImportBlocker: (value: string | null) => {
      importBlocker = value;
    },
    setRemotePolicy: (value: AgentControlPolicyStateResult) => {
      policy = value;
    },
  };
}

describe("autonomous project setup", () => {
  it("loads a fresh project, saves checks as argv, reloads, and preserves unedited policy fields", async () => {
    const h = harness();
    h.setRemotePolicy({
      ...initialPolicy,
      projectPolicy: {
        projectId,
        revision: 4,
        updatedAt: "2026-09-11T12:00:00.000Z",
        policy: {
          fullAccess: true,
          providerAllowlist: [ProviderInstanceId.make("codex")],
          defaultFallbacks: [
            {
              instanceId: ProviderInstanceId.make("codex"),
              model: "model",
              options: [{ id: "fastMode", value: true }],
            },
          ],
        },
      },
    });
    await h.controller.load();
    h.controller.setPolicy({
      ...h.controller.getSnapshot().policyDraft,
      verificationChecks: [check],
    });
    expect(h.controller.getSnapshot().policyDirty).toBe(true);
    await h.controller.savePolicy();
    expect(h.api.setPolicy).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedRevision: 4,
        policy: expect.objectContaining({
          fullAccess: true,
          providerAllowlist: ["codex"],
          verificationChecks: [check],
        }),
      }),
    );
    await h.controller.discard();
    expect(h.controller.getSnapshot().policyDraft?.verificationChecks?.[0]?.args).toEqual(
      check.args,
    );
    const { defaultFallbacks: _defaults, ...inherited } = h.controller.getSnapshot().policyDraft!;
    h.controller.setPolicy(inherited);
    await h.controller.savePolicy();
    expect(h.controller.getSnapshot().policyDraft).toEqual({
      fullAccess: true,
      providerAllowlist: ["codex"],
      verificationChecks: [check],
    });
  });
  it("does not lose a draft or silently overwrite a concurrent policy revision", async () => {
    const h = harness();
    await h.controller.load();
    h.controller.setPolicy({ verificationChecks: [check] });
    h.setRemotePolicy({
      ...initialPolicy,
      projectPolicy: {
        projectId,
        policy: { fullAccess: true },
        revision: 1,
        updatedAt: "2026-09-11T12:00:00.000Z",
      },
    });
    await h.controller.savePolicy();
    expect(h.controller.getSnapshot()).toMatchObject({
      loaded: false,
      policyDirty: true,
      policyDraft: { verificationChecks: [check] },
    });
    expect(h.controller.getSnapshot().error).toContain("changed elsewhere");
    await h.controller.savePolicy();
    expect(h.api.setPolicy).toHaveBeenCalledTimes(1);
    await h.controller.discard();
    expect(h.controller.getSnapshot().policyDraft).toEqual({ fullAccess: true });
  });
  it("retains a successful policy save when the separate GitHub step fails", async () => {
    const h = harness();
    await h.controller.load();
    h.controller.setPolicy({ verificationChecks: [check] });
    await h.controller.savePolicy();
    vi.mocked(h.api.setGithub).mockRejectedValue({ code: "github-issues-disabled" });
    await h.controller.saveGithub();
    expect(h.controller.getSnapshot()).toMatchObject({
      policyDirty: false,
      policyDraft: { verificationChecks: [check] },
    });
    expect(h.controller.getSnapshot().notice).toContain("settings saved");
    expect(h.controller.getSnapshot().error).toContain("Issues are disabled");
  });
  it("blocks duplicate writes and ignores an old environment response", async () => {
    const h = harness();
    await h.controller.load();
    const response = deferred<AgentControlPolicyStateResult>();
    vi.mocked(h.api.setPolicy).mockReturnValue(response.promise);
    h.controller.setPolicy({ verificationChecks: [check] });
    const save = h.controller.savePolicy();
    await h.controller.savePolicy();
    expect(h.api.setPolicy).toHaveBeenCalledTimes(1);
    h.setConnected(false);
    h.controller.invalidate();
    response.resolve(initialPolicy);
    await save;
    expect(h.controller.getSnapshot()).toMatchObject({ loaded: false, policyDirty: true });
    expect(h.api.onSaved).not.toHaveBeenCalled();
    h.setConnected(true);
    await h.controller.load();
    expect(h.controller.getSnapshot()).toMatchObject({ loaded: false, policyDirty: true });
    await h.controller.discard();
    expect(h.controller.getSnapshot().loaded).toBe(true);
  });
  it("guards each operation with the current API-specific permission", async () => {
    const h = harness();
    await h.controller.load();
    await h.controller.saveGithub();
    h.controller.setPolicy({ verificationChecks: [check] });
    h.setWriteBlocker("Permissions refreshing");
    await h.controller.savePolicy();
    expect(h.api.setPolicy).not.toHaveBeenCalled();
    await h.controller.importIssues();
    expect(h.api.pollOnce).toHaveBeenCalledTimes(1);
    h.setImportBlocker("No operate permission");
    await h.controller.importIssues();
    expect(h.api.pollOnce).toHaveBeenCalledTimes(1);
    h.setWriteBlocker(null);
    await h.controller.savePolicy();
    expect(h.api.setPolicy).toHaveBeenCalledTimes(1);
  });
  it("never treats a preflight or save as execution and never changes project mode", async () => {
    const h = harness();
    await h.controller.load();
    h.controller.setPolicy({ verificationChecks: [check] });
    await h.controller.preflight();
    expect(h.controller.getSnapshot().notice).toContain("have not been executed");
    expect(h.api.setPolicy).not.toHaveBeenCalled();
    expect(h.controller.getSnapshot().policyDirty).toBe(true);
  });
  it("shows a failed load and refuses writes with incomplete settings", async () => {
    const h = harness();
    vi.mocked(h.api.getGithub).mockRejectedValue(new Error("Offline"));
    await h.controller.load();
    h.controller.setPolicy({ verificationChecks: [check] });
    await h.controller.savePolicy();
    expect(h.controller.getSnapshot().loaded).toBe(false);
    expect(h.api.setPolicy).not.toHaveBeenCalled();
  });
  it("rejects invalid checks before sending them to the server", async () => {
    const h = harness();
    await h.controller.load();
    for (const bad of [
      { ...check, cwd: "../outside" },
      { ...check, timeoutMs: 300001 },
      { ...check, id: "git-status" },
    ]) {
      h.controller.setPolicy({ verificationChecks: [bad] });
      await h.controller.savePolicy();
      expect(h.api.setPolicy).not.toHaveBeenCalled();
      expect(h.controller.getSnapshot().loaded).toBe(true);
      expect(h.controller.getSnapshot().error).toContain("Invalid policy");
    }
  });
  it("keeps trusted accounts, labels and interval after import and clears intake independently", async () => {
    const h = harness();
    await h.controller.load();
    h.controller.setGithub({
      trackerKind: "github",
      readyLabel: "ready",
      pausedLabel: "paused",
      trustedLogins: ["maintainer"],
      pollIntervalSeconds: 75,
    });
    await h.controller.saveGithub();
    await h.controller.importIssues();
    expect(h.controller.getSnapshot().notice).toContain("1 issues observed");
    expect(h.controller.getSnapshot().githubDraft?.trustedLogins).toEqual(["maintainer"]);
    await h.controller.clearGithub();
    expect(h.controller.getSnapshot().githubState?.config).toBeNull();
    expect(h.api.setPolicy).not.toHaveBeenCalled();
  });
});

describe("setup session permissions", () => {
  const session = (scopes: AuthSessionState["scopes"]): AuthSessionState =>
    ({ authenticated: true, scopes }) as AuthSessionState;
  it("denies unknown, refreshing and insufficient rights", () => {
    expect(agentControlSetupPermissionBlocker(AsyncResult.initial(), "configure")).not.toBeNull();
    expect(
      agentControlSetupPermissionBlocker(
        AsyncResult.waiting(AsyncResult.success(session([AuthAccessWriteScope]))),
        "configure",
      ),
    ).not.toBeNull();
    expect(
      agentControlSetupPermissionBlocker(
        AsyncResult.success(session([AuthOrchestrationOperateScope])),
        "configure",
      ),
    ).not.toBeNull();
    expect(
      agentControlSetupPermissionBlocker(
        AsyncResult.success(session([AuthOrchestrationOperateScope])),
        "import",
      ),
    ).toBeNull();
    expect(
      agentControlSetupPermissionBlocker(
        AsyncResult.success(session([AuthAccessWriteScope])),
        "configure",
      ),
    ).toBeNull();
    expect(
      agentControlSetupPermissionBlocker(
        AsyncResult.success(session([AuthAccessWriteScope])),
        "import",
      ),
    ).not.toBeNull();
  });
});
