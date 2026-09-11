import {
  AGENT_CONTROL_GITHUB_RPC_METHODS,
  AGENT_CONTROL_RPC_METHODS,
  AgentControlProjectPolicy,
  AgentControlGithubTrackerSettings,
  AuthAccessWriteScope,
  AuthOrchestrationOperateScope,
  CommandId,
  type AgentControlGithubCommandResult,
  type AgentControlGithubIntakeState,
  type AgentControlGithubSetTrackerConfigInput,
  type AgentControlGithubClearTrackerConfigInput,
  type AgentControlGithubPollOnceInput,
  type AgentControlPolicyStateResult,
  type AgentControlPreflightRuntimeInput,
  type AgentControlPreflightRuntimeResult,
  type AgentControlSetProjectPolicyInput,
  type AuthSessionState,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createEnvironmentRpcCommand,
  squashAtomCommandFailure,
  type AtomCommand,
} from "./runtime.ts";
import { agentControlCommandErrorMessage } from "./agentControl.ts";

export function createAgentControlSetupEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    getPolicy: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:load-policy",
      tag: AGENT_CONTROL_RPC_METHODS.getPolicy,
    }),
    getGithub: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:load-github",
      tag: AGENT_CONTROL_GITHUB_RPC_METHODS.getObserveState,
    }),
    setPolicy: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:save-policy",
      tag: AGENT_CONTROL_RPC_METHODS.setProjectPolicy,
    }),
    setGithub: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:save-github",
      tag: AGENT_CONTROL_GITHUB_RPC_METHODS.setTrackerConfig,
    }),
    clearGithub: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:clear-github",
      tag: AGENT_CONTROL_GITHUB_RPC_METHODS.clearTrackerConfig,
    }),
    pollOnce: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:import",
      tag: AGENT_CONTROL_GITHUB_RPC_METHODS.pollOnce,
    }),
    preflight: createEnvironmentRpcCommand(runtime, {
      label: "agent-control-setup:preflight",
      tag: AGENT_CONTROL_RPC_METHODS.preflightRuntime,
    }),
  };
}

export function agentControlSetupPermissionBlocker<E>(
  session: AsyncResult.AsyncResult<AuthSessionState, E>,
  action: "configure" | "import",
): string | null {
  if (session._tag !== "Success" || session.waiting)
    return "Waiting for current session permissions in this environment. Reconnect if this persists.";
  const scope = action === "configure" ? AuthAccessWriteScope : AuthOrchestrationOperateScope;
  if (!session.value.authenticated || !session.value.scopes?.includes(scope))
    return `Your session in this environment needs ${scope} to ${action === "configure" ? "save configuration" : "import GitHub issues"}. Ask the environment administrator for a pairing link with this permission.`;
  return null;
}

/** Shared explanations for the existing policy and provider preflight codes. */
export function agentControlPreflightErrorMessage(code: string): string {
  const messages: Record<string, string> = {
    "provider-not-configured":
      "This provider instance is not configured in this environment. Choose an existing instance.",
    "provider-instance-missing":
      "This provider instance is no longer available in this environment. Choose another instance.",
    "provider-driver-unavailable":
      "This environment does not support this provider driver. Choose a supported instance.",
    "provider-disabled":
      "This provider instance is disabled in the environment's provider settings.",
    "provider-not-installed":
      "The provider runtime is not installed on this environment. Complete its existing provider setup.",
    "provider-not-ready":
      "The provider is not ready. Review its status in this environment's provider settings.",
    "provider-unauthenticated":
      "The provider is not signed in on this environment. Use its existing provider sign-in flow.",
    "provider-probe-timeout":
      "The provider readiness check timed out. Check the environment and try again.",
    "provider-probe-failed":
      "The provider readiness check failed. Review its status in provider settings and retry.",
    "model-unavailable":
      "This model is unavailable for the selected provider instance. Choose an available model.",
    "driver-kind-mismatch":
      "This provider does not match the role's saved driver constraint. Choose a matching instance or review that constraint.",
    "provider-not-allowed":
      "This provider is excluded by the project's effective provider allowlist. Review that list or choose an allowed instance.",
    "role-unresolved":
      "This role has no valid route. Configure a role candidate or an allowed default fallback.",
    "role-runtime-unresolved":
      "No candidate for this role is ready. Resolve the candidate errors below or configure an available fallback.",
  };
  return (
    messages[code] ??
    `Provider readiness failed (${code}). Review the environment's provider settings.`
  );
}

export function agentControlSetupErrorMessage(error: unknown): string {
  const code = typeof error === "object" && error !== null && "code" in error ? error.code : null;
  const messages: Record<string, string> = {
    "github-issues-disabled":
      "Issues are disabled in the checkout's GitHub repository. Select a project whose bound repository has Issues enabled; this screen does not change GitHub repository settings.",
    "github-authentication":
      "GitHub authentication failed on this environment. Sign in to GitHub using the environment's existing GitHub connection, then retry.",
    "github-unavailable":
      "The GitHub CLI is unavailable on this environment. Install or configure it using the existing environment setup.",
    "github-command-failed":
      "GitHub could not read the bound repository. Check the environment's GitHub account and repository access.",
    "repository-not-github": "The checkout's selected Git remote is not a GitHub repository.",
    "repository-identity-conflict":
      "The checkout's repository differs from the saved intake binding. Review the Git remotes and remove the old intake configuration before rebinding.",
    "repository-identity-changed":
      "The checkout's GitHub repository changed. Review its remotes and saved intake binding before importing.",
    validation:
      "The server rejected this configuration. Check labels, trusted accounts, routing and verification fields.",
    "revision-conflict":
      "This project was changed elsewhere. Your draft is retained. Reload saved settings before editing and saving again.",
  };
  return typeof code === "string" && messages[code]
    ? messages[code]
    : agentControlCommandErrorMessage(error);
}

export interface AgentControlSetupApi {
  projectId: ProjectId;
  getPolicy(): Promise<AgentControlPolicyStateResult>;
  getGithub(): Promise<AgentControlGithubIntakeState>;
  setPolicy(input: AgentControlSetProjectPolicyInput): Promise<AgentControlPolicyStateResult>;
  setGithub(
    input: AgentControlGithubSetTrackerConfigInput,
  ): Promise<AgentControlGithubCommandResult>;
  clearGithub(
    input: AgentControlGithubClearTrackerConfigInput,
  ): Promise<AgentControlGithubCommandResult>;
  pollOnce(input: AgentControlGithubPollOnceInput): Promise<AgentControlGithubCommandResult>;
  preflight(input: AgentControlPreflightRuntimeInput): Promise<AgentControlPreflightRuntimeResult>;
  canWrite(): string | null;
  canImport(): string | null;
  isCurrent(): boolean;
  onSaved?(): void;
}

type SetupCommands = ReturnType<typeof createAgentControlSetupEnvironmentAtoms>;
export function bindAgentControlSetupApi(
  commands: SetupCommands,
  registry: AtomRegistry.AtomRegistry,
  target: { environmentId: EnvironmentId; input: { projectId: ProjectId } },
  access: Pick<AgentControlSetupApi, "canWrite" | "canImport" | "isCurrent" | "onSaved">,
): AgentControlSetupApi {
  const bind =
    <I, A, E>(command: AtomCommand<{ environmentId: EnvironmentId; input: I }, A, E>) =>
    async (input: I): Promise<A> => {
      const result = await command.run(registry, { environmentId: target.environmentId, input });
      if (result._tag === "Failure") throw squashAtomCommandFailure(result);
      return result.value;
    };
  return {
    ...access,
    projectId: target.input.projectId,
    getPolicy: () => bind(commands.getPolicy)(target.input),
    getGithub: () => bind(commands.getGithub)(target.input),
    setPolicy: bind(commands.setPolicy),
    setGithub: bind(commands.setGithub),
    clearGithub: bind(commands.clearGithub),
    pollOnce: bind(commands.pollOnce),
    preflight: bind(commands.preflight),
  };
}

export interface AgentControlSetupSnapshot {
  policyState: AgentControlPolicyStateResult | null;
  githubState: AgentControlGithubIntakeState | null;
  policyDraft: AgentControlProjectPolicy | null;
  githubDraft: AgentControlGithubTrackerSettings | null;
  policyDirty: boolean;
  githubDirty: boolean;
  loaded: boolean;
  pending: boolean;
  error: string | null;
  notice: string | null;
  preflight: AgentControlPreflightRuntimeResult | null;
}

const defaultGithub = (): AgentControlGithubTrackerSettings => ({
  trackerKind: "github",
  readyLabel: "agent:ready",
  pausedLabel: "agent:paused",
  trustedLogins: [],
  pollIntervalSeconds: 60,
});
const isProjectPolicy = Schema.is(AgentControlProjectPolicy);
const isGithubSettings = Schema.is(AgentControlGithubTrackerSettings);
class DraftValidationError extends Error {}
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** A project/environment owns one draft and one in-flight operation. Failed writes require an explicit reload; no silent rebasing. */
export function createAgentControlSetupController(api: AgentControlSetupApi) {
  let state: AgentControlSetupSnapshot = {
    policyState: null,
    githubState: null,
    policyDraft: null,
    githubDraft: null,
    policyDirty: false,
    githubDirty: false,
    loaded: false,
    pending: false,
    error: null,
    notice: null,
    preflight: null,
  };
  const listeners = new Set<() => void>();
  let generation = 0;
  const update = (patch: Partial<AgentControlSetupSnapshot>) => {
    state = { ...state, ...patch };
    listeners.forEach((listener) => listener());
  };
  const current = (token: number) => generation === token && api.isCurrent();
  const guard = (kind: "configure" | "import" | "read") => {
    if (state.pending) return false;
    const reason = !api.isCurrent()
      ? "Reconnect to this environment before continuing."
      : !state.loaded
        ? "Reload saved settings before continuing. Your draft has been retained."
        : kind === "configure"
          ? api.canWrite()
          : kind === "import"
            ? api.canImport()
            : null;
    if (reason) {
      update({ error: reason });
      return false;
    }
    return true;
  };
  async function load(discard = false) {
    if (state.pending || !api.isCurrent()) return;
    if (!discard && (state.policyDirty || state.githubDirty)) {
      update({
        loaded: false,
        error:
          "The connection changed. Your draft is retained; reload saved settings before saving again.",
      });
      return;
    }
    const token = ++generation;
    update({ pending: true, error: null, loaded: false });
    const [policy, github] = await Promise.allSettled([api.getPolicy(), api.getGithub()]);
    if (!current(token)) return;
    update({
      pending: false,
      loaded: policy.status === "fulfilled" && github.status === "fulfilled",
      policyState: policy.status === "fulfilled" ? policy.value : null,
      githubState: github.status === "fulfilled" ? github.value : null,
      policyDraft:
        policy.status === "fulfilled" ? (policy.value.projectPolicy?.policy ?? {}) : null,
      githubDraft:
        github.status === "fulfilled" ? (github.value.config?.settings ?? defaultGithub()) : null,
      policyDirty: false,
      githubDirty: false,
      preflight: null,
      error:
        [policy, github]
          .flatMap((result) =>
            result.status === "rejected" ? [agentControlSetupErrorMessage(result.reason)] : [],
          )
          .join(" ") || null,
      notice: discard ? "Saved settings reloaded; local edits discarded." : null,
    });
  }
  async function operation(
    kind: "configure" | "import" | "read",
    work: (token: number) => Promise<void>,
    isWrite = true,
  ) {
    if (!guard(kind)) return;
    const token = generation;
    update({ pending: true, error: null });
    try {
      await work(token);
    } catch (error) {
      if (current(token))
        update({
          error: agentControlSetupErrorMessage(error),
          ...(isWrite && !(error instanceof DraftValidationError) ? { loaded: false } : {}),
        });
    } finally {
      if (current(token)) update({ pending: false });
    }
  }
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    load: () => load(),
    discard: () => load(true),
    invalidate: () => {
      generation++;
      update({ loaded: false, pending: false, preflight: null });
    },
    setPolicy: (policy: AgentControlProjectPolicy) => {
      if (!state.pending)
        update({
          policyDraft: policy,
          policyDirty: !same(policy, state.policyState?.projectPolicy?.policy ?? {}),
          preflight: null,
        });
    },
    setGithub: (settings: AgentControlGithubTrackerSettings) => {
      if (!state.pending)
        update({
          githubDraft: settings,
          githubDirty: !same(settings, state.githubState?.config?.settings ?? defaultGithub()),
        });
    },
    savePolicy: () =>
      operation("configure", async (token) => {
        if (!state.policyDraft || !state.policyDirty) return;
        if (!isProjectPolicy(state.policyDraft))
          throw new DraftValidationError(
            "Invalid policy. Check unique check IDs (lowercase, no git- prefix), program, argument list, relative worktree directory, timeout (1–300000 ms), and nonempty role routes.",
          );
        const result = await api.setPolicy({
          projectId: api.projectId,
          expectedRevision: state.policyState?.projectPolicy?.revision ?? 0,
          policy: state.policyDraft,
        });
        if (!current(token)) return;
        update({
          policyState: result,
          policyDraft: result.projectPolicy?.policy ?? {},
          policyDirty: false,
          preflight: null,
          notice:
            "Provider routing and verification settings saved. Checks have not been executed. GitHub settings are saved separately.",
        });
        api.onSaved?.();
      }),
    saveGithub: () =>
      operation("configure", async (token) => {
        if (!state.githubDraft) return;
        if (!isGithubSettings(state.githubDraft))
          throw new DraftValidationError(
            "Invalid GitHub settings. Labels and accounts must be nonempty; poll interval must be an integer from 15 to 3600 seconds.",
          );
        const result = await api.setGithub({
          ...state.githubDraft,
          projectId: api.projectId,
          expectedRevision: state.githubState!.revision,
          commandId: CommandId.make(
            `t3auto-setup-github:${JSON.stringify([api.projectId, state.githubState!.revision, state.githubDraft])}`,
          ),
        });
        if (!current(token)) return;
        update({
          githubState: result.state,
          githubDraft: result.state.config?.settings ?? defaultGithub(),
          githubDirty: false,
          notice:
            "GitHub intake configuration saved and repository binding checked. Provider routing and checks are saved separately. Run once and Armed were not enabled.",
        });
        api.onSaved?.();
      }),
    clearGithub: () =>
      operation("configure", async (token) => {
        const result = await api.clearGithub({
          projectId: api.projectId,
          expectedRevision: state.githubState!.revision,
          commandId: CommandId.make(
            `t3auto-setup-clear:${api.projectId}:${state.githubState!.revision}`,
          ),
        });
        if (!current(token)) return;
        update({
          githubState: result.state,
          githubDraft: defaultGithub(),
          githubDirty: false,
          notice: "GitHub intake configuration removed. Project policy is unchanged.",
        });
        api.onSaved?.();
      }),
    importIssues: () =>
      operation("import", async (token) => {
        if (!state.githubState?.config || state.githubDirty)
          throw new DraftValidationError("Save the GitHub configuration before importing issues.");
        const result = await api.pollOnce({
          projectId: api.projectId,
          expectedRevision: state.githubState.revision,
          commandId: CommandId.make(
            `t3auto-setup-poll:${api.projectId}:${state.githubState.revision}:${state.githubState.sequence}`,
          ),
        });
        if (!current(token)) return;
        update({ githubState: result.state });
        if (result.state.pollStatus.status === "needs-attention")
          throw { code: result.state.pollStatus.errorCode };
        update({
          notice:
            result.state.pollStatus.status === "success"
              ? `GitHub import completed: ${result.state.pollStatus.issueCount} issues observed. Task eligibility is determined by labels and trusted accounts; no run was started.`
              : "GitHub import did not complete. Review intake status.",
        });
        api.onSaved?.();
      }),
    preflight: () =>
      operation(
        "read",
        async (token) => {
          if (!state.policyDraft) return;
          const result = await api.preflight({
            projectId: api.projectId,
            projectPolicy: state.policyDraft,
          });
          if (!current(token)) return;
          update({
            preflight: result,
            notice: result.ok
              ? "Provider preflight passed for this draft. Verification checks have not been executed and settings have not been saved by preflight."
              : "Provider preflight found blockers. Verification checks have not been executed.",
          });
        },
        false,
      ),
  };
}

export type AgentControlSetupController = ReturnType<typeof createAgentControlSetupController>;
