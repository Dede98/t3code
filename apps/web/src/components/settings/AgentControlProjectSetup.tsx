import { RegistryContext } from "@effect/atom-react";
import {
  bindAgentControlSetupApi,
  agentControlPreflightErrorMessage,
  createAgentControlSetupController,
  createAgentControlSetupEnvironmentAtoms,
} from "@t3tools/client-runtime/state/agent-control-setup";
import {
  AGENT_CONTROL_ROLES,
  type AgentControlProjectPolicy,
  type AgentControlVerificationCheck,
  type ModelSelection,
  type ServerProvider,
  type EnvironmentId,
  type ProjectId,
  type RepositoryIdentity,
} from "@t3tools/contracts";
import {
  useContext,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
  type ReactNode,
} from "react";

import { connectionAtomRuntime } from "../../connection/runtime";
import { Button } from "../ui/button";

type Controller = ReturnType<typeof createAgentControlSetupController>;
const setupCommands = createAgentControlSetupEnvironmentAtoms(connectionAtomRuntime);
const inputClass = "w-full rounded-md border bg-background px-2 py-1.5 text-sm";

export function AgentControlProjectSetupPanel(props: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  connected: boolean;
  repositoryIdentity: RepositoryIdentity | null;
  providers: readonly ServerProvider[];
  writeBlocker: string | null;
  importBlocker: string | null;
  activeRun: boolean;
  onSaved: () => void;
  onStatusChange: (status: { pending: boolean; dirty: boolean }) => void;
}) {
  const { environmentId, projectId, connected, onStatusChange } = props;
  const registry = useContext(RegistryContext);
  const latest = useRef(props);
  useLayoutEffect(() => {
    latest.current = props;
  });
  const mounted = useRef(false);
  const canWrite = useCallback(() => latest.current.writeBlocker, []);
  const canImport = useCallback(() => latest.current.importBlocker, []);
  const isCurrent = useCallback(() => mounted.current && latest.current.connected, []);
  const onSaved = useCallback(() => latest.current.onSaved(), []);
  const controller = useMemo(
    () =>
      createAgentControlSetupController(
        bindAgentControlSetupApi(
          setupCommands,
          registry,
          { environmentId, input: { projectId } },
          // These callbacks are stored here and read only when the controller handles an action.
          // eslint-disable-next-line react/refs
          {
            canWrite,
            canImport,
            isCurrent,
            onSaved,
          },
        ),
      ),
    [registry, environmentId, projectId, canWrite, canImport, isCurrent, onSaved],
  );
  const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  const dirty = state.policyDirty || state.githubDirty;
  useLayoutEffect(() => {
    onStatusChange({ pending: state.pending, dirty });
  }, [onStatusChange, state.pending, dirty]);
  useEffect(() => {
    mounted.current = true;
    if (connected) void controller.load();
    else controller.invalidate();
    return () => {
      mounted.current = false;
      controller.invalidate();
    };
  }, [controller, connected]);
  return <AgentControlProjectSetup controller={controller} state={state} {...props} />;
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="block space-y-1 text-sm">
      <span>{label}</span>
      {children}
    </label>
  );
}

function StringList({
  label,
  values,
  onChange,
}: {
  label: string;
  values: readonly string[];
  onChange: (values: string[]) => void;
}) {
  return (
    <div className="space-y-2">
      <p className="text-sm">{label}</p>
      {values.map((value, index) => (
        // Controlled rows have no local state; their editable values cannot serve as stable keys.
        // eslint-disable-next-line react/no-array-index-key
        <div key={index} className="flex items-start gap-2">
          <textarea
            aria-label={`${label} ${index + 1}`}
            rows={1}
            className={inputClass}
            value={value}
            onChange={(event) =>
              onChange(values.map((item, i) => (i === index ? event.target.value : item)))
            }
          />
          <Button
            size="xs"
            variant="outline"
            onClick={() => onChange(values.filter((_, i) => i !== index))}
          >
            Remove
          </Button>
        </div>
      ))}
      <Button size="xs" variant="outline" onClick={() => onChange([...values, ""])}>
        Add {label.toLowerCase().replace(/s$/, "")}
      </Button>
    </div>
  );
}

function Candidates({
  label,
  values,
  providers,
  onChange,
}: {
  label: string;
  values: readonly ModelSelection[];
  providers: readonly ServerProvider[];
  onChange: (values: ModelSelection[]) => void;
}) {
  const initialProvider = providers.find(
    (provider) => provider.enabled && provider.models.length > 0,
  );
  return (
    <div className="space-y-2">
      {values.map((value, index) => {
        const provider = providers.find((item) => item.instanceId === value.instanceId);
        const update = (selection: ModelSelection) =>
          onChange(values.map((item, i) => (i === index ? selection : item)));
        return (
          // Candidates can repeat and all their fields are editable; row position owns the input.
          // eslint-disable-next-line react/no-array-index-key
          <div key={index} className="space-y-2 rounded border p-2">
            <div className="grid gap-2 sm:grid-cols-2">
              <Field label={`${label} ${index + 1}: provider`}>
                <select
                  className={inputClass}
                  value={value.instanceId}
                  onChange={(event) => {
                    const next = providers.find((item) => item.instanceId === event.target.value);
                    if (next)
                      update({ instanceId: next.instanceId, model: next.models[0]?.slug ?? "" });
                  }}
                >
                  {!provider ? (
                    <option value={value.instanceId}>{value.instanceId} (unavailable)</option>
                  ) : null}
                  {providers.map((item) => (
                    <option key={item.instanceId} value={item.instanceId}>
                      {item.displayName ?? item.instanceId}
                      {!item.enabled ? " (disabled)" : ""}
                    </option>
                  ))}
                </select>
              </Field>
              <Field label={`${label} ${index + 1}: model`}>
                <select
                  className={inputClass}
                  value={value.model}
                  onChange={(event) => update({ ...value, model: event.target.value })}
                >
                  {!provider?.models.some((model) => model.slug === value.model) ? (
                    <option value={value.model}>
                      {value.model || "Choose a model"} (unavailable)
                    </option>
                  ) : null}
                  {provider?.models.map((model) => (
                    <option key={model.slug} value={model.slug}>
                      {model.name}
                    </option>
                  ))}
                </select>
              </Field>
            </div>
            <div className="flex gap-2">
              <Button
                size="xs"
                variant="outline"
                disabled={index === 0}
                onClick={() => {
                  const next = [...values];
                  next.splice(index, 1);
                  next.splice(index - 1, 0, value);
                  onChange(next);
                }}
              >
                Move earlier
              </Button>
              <Button
                size="xs"
                variant="outline"
                onClick={() => onChange(values.filter((_, i) => i !== index))}
              >
                Remove candidate
              </Button>
            </div>
          </div>
        );
      })}
      <Button
        size="xs"
        variant="outline"
        disabled={!initialProvider}
        onClick={() => {
          if (initialProvider)
            onChange([
              ...values,
              { instanceId: initialProvider.instanceId, model: initialProvider.models[0]!.slug },
            ]);
        }}
      >
        Add candidate
      </Button>
      {!initialProvider ? (
        <p className="text-sm text-muted-foreground">
          Configure a provider with models in this environment’s provider settings first.
        </p>
      ) : null}
    </div>
  );
}

function CheckEditor({
  check,
  onChange,
  onRemove,
}: {
  check: AgentControlVerificationCheck;
  onChange: (check: AgentControlVerificationCheck) => void;
  onRemove: () => void;
}) {
  return (
    <div className="space-y-3 rounded-md border p-3">
      <div className="grid gap-3 sm:grid-cols-2">
        <Field label="Check ID">
          <input
            className={inputClass}
            value={check.id}
            onChange={(event) => onChange({ ...check, id: event.target.value })}
          />
        </Field>
        <Field label="Executable program">
          <input
            className={inputClass}
            value={check.command}
            onChange={(event) => onChange({ ...check, command: event.target.value })}
            placeholder="node"
          />
        </Field>
        <Field label="Working directory (relative to task worktree)">
          <input
            className={inputClass}
            value={check.cwd}
            onChange={(event) => onChange({ ...check, cwd: event.target.value })}
          />
        </Field>
        <Field label="Timeout (milliseconds, maximum 300000)">
          <input
            type="number"
            min={1}
            max={300000}
            className={inputClass}
            value={check.timeoutMs}
            onChange={(event) => onChange({ ...check, timeoutMs: Number(event.target.value) })}
          />
        </Field>
        <Field label="Result format">
          <select
            className={inputClass}
            value={check.resultFormat}
            onChange={(event) => {
              const value = event.target.value;
              if (value === "exit-code" || value === "node-test" || value === "vitest-json")
                onChange({ ...check, resultFormat: value });
            }}
          >
            <option value="exit-code">Exit code</option>
            <option value="node-test">Node test</option>
            <option value="vitest-json">Vitest JSON</option>
          </select>
        </Field>
      </div>
      <StringList
        label="Arguments"
        values={check.args}
        onChange={(args) => onChange({ ...check, args })}
      />
      <p className="text-xs text-muted-foreground">
        Each row is one argument, including spaces and line breaks. No shell splitting is applied.
      </p>
      <label className="flex gap-2 text-sm">
        <input
          type="checkbox"
          checked={check.required}
          onChange={(event) => onChange({ ...check, required: event.target.checked })}
        />
        Required for successful verification
      </label>
      <label className="flex gap-2 text-sm">
        <input
          type="checkbox"
          checked={check.allowTemporaryFiles}
          onChange={(event) => onChange({ ...check, allowTemporaryFiles: event.target.checked })}
        />
        Allow temporary files during this check
      </label>
      <Button size="xs" variant="outline" onClick={onRemove}>
        Remove check
      </Button>
    </div>
  );
}

export function AgentControlProjectSetup({
  controller,
  state,
  providers,
  writeBlocker,
  importBlocker,
  activeRun,
  repositoryIdentity,
}: {
  controller: Controller;
  state: ReturnType<Controller["getSnapshot"]>;
  providers: readonly ServerProvider[];
  writeBlocker: string | null;
  importBlocker: string | null;
  activeRun: boolean;
  repositoryIdentity: RepositoryIdentity | null;
}) {
  const policy = state.policyDraft;
  const github = state.githubDraft;
  const updatePolicy = (patch: Partial<AgentControlProjectPolicy>) => {
    if (policy) controller.setPolicy({ ...policy, ...patch });
  };
  const inherit = (key: keyof AgentControlProjectPolicy) => {
    if (!policy) return;
    const next = { ...policy };
    delete next[key];
    controller.setPolicy(next);
  };
  return (
    <details className="space-y-3 rounded-md border p-3" open>
      <summary className="cursor-pointer text-sm font-medium">
        Project setup ·{" "}
        {state.policyDirty || state.githubDirty
          ? "Unsaved changes"
          : state.loaded
            ? "Saved settings"
            : "Loading settings"}
      </summary>
      <p className="text-sm text-muted-foreground">
        Changes apply only to the project and environment shown above. GitHub intake and project
        policy are saved separately. Saving does not turn on Run once or Armed.
      </p>
      {activeRun ? (
        <p className="text-sm text-muted-foreground">
          A run is active. Finish or end it before changing setup: changes could affect later
          stages. Saved changes apply when subsequent work starts.
        </p>
      ) : null}
      {writeBlocker ? (
        <p role="status" className="text-sm text-amber-600">
          {writeBlocker}
        </p>
      ) : null}
      {state.error ? (
        <p role="alert" className="break-words text-sm text-destructive">
          {state.error}
        </p>
      ) : null}
      {state.notice ? (
        <p role="status" className="text-sm">
          {state.notice}
        </p>
      ) : null}
      {state.pending ? (
        <p role="status" className="text-sm">
          Waiting for the environment…
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        <Button
          size="xs"
          variant="outline"
          disabled={state.pending}
          onClick={() => controller.discard()}
        >
          Discard unsaved changes
        </Button>
        <Button
          size="xs"
          variant="outline"
          disabled={state.pending || state.policyDirty || state.githubDirty}
          onClick={() => void controller.load()}
        >
          Reload saved settings
        </Button>
      </div>
      <fieldset
        disabled={state.pending || writeBlocker !== null}
        className="space-y-4 disabled:opacity-60"
      >
        <legend className="text-sm font-medium">GitHub intake</legend>
        <p className="break-all text-sm">
          Checkout repository (last resolved):{" "}
          {repositoryIdentity?.canonicalKey ??
            "No repository identity available for this checkout."}
          {repositoryIdentity ? ` · Git remote: ${repositoryIdentity.locator.remoteName}` : ""}
        </p>
        <p className="break-all text-sm">
          Saved GitHub intake binding:{" "}
          {state.githubState?.config?.repository.nameWithOwner ?? "Not configured"}
        </p>
        <p className="text-xs text-muted-foreground">
          The environment selects the checkout’s upstream remote, then origin, then the first
          remaining remote by name. Saving rechecks this repository and its GitHub issue access.
          GitHub CLI authentication is required on that environment.
        </p>
        {github ? (
          <>
            <div className="grid gap-3 sm:grid-cols-2">
              <Field label="Ready label">
                <input
                  className={inputClass}
                  value={github.readyLabel}
                  onChange={(event) =>
                    controller.setGithub({ ...github, readyLabel: event.target.value })
                  }
                />
              </Field>
              <Field label="Pause label">
                <input
                  className={inputClass}
                  value={github.pausedLabel}
                  onChange={(event) =>
                    controller.setGithub({ ...github, pausedLabel: event.target.value })
                  }
                />
              </Field>
              <Field label="Poll interval (seconds, 15–3600)">
                <input
                  type="number"
                  min={15}
                  max={3600}
                  className={inputClass}
                  value={github.pollIntervalSeconds}
                  onChange={(event) =>
                    controller.setGithub({
                      ...github,
                      pollIntervalSeconds: Number(event.target.value),
                    })
                  }
                />
              </Field>
            </div>
            <StringList
              label="Trusted accounts"
              values={github.trustedLogins}
              onChange={(trustedLogins) => controller.setGithub({ ...github, trustedLogins })}
            />
            <p className="text-xs text-muted-foreground">
              Only ready-label events from these GitHub accounts authorize task intake.
            </p>
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                disabled={!state.githubDirty && state.githubState?.config != null}
                onClick={() => void controller.saveGithub()}
              >
                Save GitHub intake
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={!state.githubState?.config}
                onClick={() => void controller.clearGithub()}
              >
                Remove GitHub intake
              </Button>
            </div>
          </>
        ) : (
          <p className="text-sm">Loading GitHub settings…</p>
        )}
      </fieldset>
      <div className="space-y-2">
        <Button
          size="sm"
          variant="outline"
          disabled={
            state.pending ||
            importBlocker !== null ||
            state.githubDirty ||
            !state.githubState?.config
          }
          onClick={() => void controller.importIssues()}
        >
          Import / refresh GitHub issues
        </Button>
        {importBlocker ? <p className="text-xs text-muted-foreground">{importBlocker}</p> : null}
        {state.githubState ? (
          <p className="text-sm">
            Last import: {state.githubState.pollStatus.status}
            {state.githubState.pollStatus.status === "success"
              ? ` · ${state.githubState.pollStatus.issueCount} issues · ${state.githubState.pollStatus.completedAt}`
              : ""}
            {state.githubState.pollStatus.errorCode
              ? ` · ${state.githubState.pollStatus.errorCode}`
              : ""}
          </p>
        ) : null}
      </div>
      <fieldset
        disabled={state.pending || writeBlocker !== null}
        className="space-y-4 disabled:opacity-60"
      >
        <legend className="text-sm font-medium">Provider routing and verification</legend>
        {policy ? (
          <>
            <details className="space-y-3" open>
              <summary className="cursor-pointer text-sm">
                Default candidates ·{" "}
                {policy.defaultFallbacks === undefined ? "Inherited" : "Project override"}
              </summary>
              <p className="text-xs text-muted-foreground">
                Candidates are considered in order for every role, after any role candidates. A
                strict role uses only its own candidates.
              </p>
              {policy.defaultFallbacks === undefined ? (
                <>
                  <p className="break-words text-sm">
                    {(
                      state.policyState?.appPolicy?.defaultFallbacks ??
                      state.policyState?.preflight.roles
                        .find((role) => !role.strict)
                        ?.validCandidates.filter(
                          (candidate) => candidate.source === "default-fallback",
                        )
                        .map((candidate) => candidate.selection)
                    )
                      ?.map((candidate) => `${candidate.instanceId} / ${candidate.model}`)
                      .join(" → ") ||
                      "No resolved inherited candidates. Check provider readiness below."}
                  </p>
                  <Button
                    size="xs"
                    variant="outline"
                    onClick={() =>
                      updatePolicy({
                        defaultFallbacks: state.policyState?.appPolicy?.defaultFallbacks ?? [],
                      })
                    }
                  >
                    Customize default candidates
                  </Button>
                </>
              ) : (
                <>
                  <Candidates
                    label="Default candidate"
                    values={policy.defaultFallbacks}
                    providers={providers}
                    onChange={(defaultFallbacks) => updatePolicy({ defaultFallbacks })}
                  />
                  <Button size="xs" variant="outline" onClick={() => inherit("defaultFallbacks")}>
                    Use inherited default candidates
                  </Button>
                </>
              )}
            </details>
            <details className="space-y-3">
              <summary className="cursor-pointer text-sm">Role overrides</summary>
              {AGENT_CONTROL_ROLES.map((role) => {
                const route = policy.roleRoutes?.[role];
                const inherited = state.policyState?.appPolicy?.roleRoutes?.[role];
                return (
                  <div key={role} className="space-y-2 rounded border p-3">
                    <p className="text-sm font-medium capitalize">
                      {role} · {route ? "Project override" : "Inherited"}
                    </p>
                    {route ? (
                      <>
                        <Candidates
                          label={role}
                          values={route.candidates}
                          providers={providers}
                          onChange={(candidates) =>
                            updatePolicy({
                              roleRoutes: {
                                ...policy.roleRoutes,
                                [role]: { ...route, candidates },
                              },
                            })
                          }
                        />
                        <label className="flex gap-2 text-sm">
                          <input
                            type="checkbox"
                            checked={route.strict}
                            onChange={(event) =>
                              updatePolicy({
                                roleRoutes: {
                                  ...policy.roleRoutes,
                                  [role]: { ...route, strict: event.target.checked },
                                },
                              })
                            }
                          />
                          Strict: do not use default fallback candidates
                        </label>
                        {route.driverKind ? (
                          <p className="text-xs">
                            Driver restriction: {route.driverKind} (preserved)
                          </p>
                        ) : null}
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() => {
                            const roleRoutes = { ...policy.roleRoutes };
                            delete roleRoutes[role];
                            updatePolicy({ roleRoutes });
                          }}
                        >
                          Use inherited role
                        </Button>
                      </>
                    ) : (
                      <>
                        <p className="break-words text-sm">
                          {inherited?.candidates
                            .map((candidate) => `${candidate.instanceId} / ${candidate.model}`)
                            .join(" → ") || "Uses default candidates"}
                          {inherited?.strict ? " · strict" : ""}
                        </p>
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() =>
                            updatePolicy({
                              roleRoutes: {
                                ...policy.roleRoutes,
                                [role]: inherited ?? {
                                  candidates: policy.defaultFallbacks?.slice(0, 1) ?? [],
                                  strict: false,
                                },
                              },
                            })
                          }
                        >
                          Customize {role}
                        </Button>
                      </>
                    )}
                  </div>
                );
              })}
            </details>
            {policy.providerAllowlist !== undefined ||
            state.policyState?.appPolicy?.providerAllowlist !== undefined ? (
              <p className="text-xs text-muted-foreground">
                Allowed provider instances (
                {policy.providerAllowlist === undefined ? "inherited" : "project"}):{" "}
                {(
                  policy.providerAllowlist ?? state.policyState?.appPolicy?.providerAllowlist
                )?.join(", ") || "None"}
                . This restriction is preserved.
              </p>
            ) : null}
            <Field label="Implementation and repair access">
              <select
                className={inputClass}
                value={
                  policy.fullAccess === undefined
                    ? "inherit"
                    : policy.fullAccess
                      ? "full"
                      : "restricted"
                }
                onChange={(event) =>
                  event.target.value === "inherit"
                    ? inherit("fullAccess")
                    : updatePolicy({ fullAccess: event.target.value === "full" })
                }
              >
                <option value="inherit">Default (restricted)</option>
                <option value="restricted">Project: restricted</option>
                <option value="full">Project: full access</option>
              </select>
            </Field>
            <p className="text-xs text-muted-foreground">
              All other roles remain restricted. Existing approval and execution limits still apply.
            </p>
            <div className="space-y-3">
              <h4 className="text-sm font-medium">Verification checks</h4>
              <p className="text-sm text-muted-foreground">
                Checks execute on this environment inside the task worktree. Saving and provider
                preflight do not execute checks. A successful run needs passing required checks.
              </p>
              {(policy.verificationChecks ?? []).map((check, index) => (
                <CheckEditor
                  // Check IDs are editable; this controlled editor has no local state.
                  // eslint-disable-next-line react/no-array-index-key
                  key={index}
                  check={check}
                  onChange={(next) =>
                    updatePolicy({
                      verificationChecks:
                        policy.verificationChecks?.map((item, i) => (i === index ? next : item)) ??
                        [],
                    })
                  }
                  onRemove={() =>
                    updatePolicy({
                      verificationChecks:
                        policy.verificationChecks?.filter((_, i) => i !== index) ?? [],
                    })
                  }
                />
              ))}
              <Button
                size="xs"
                variant="outline"
                disabled={(policy.verificationChecks?.length ?? 0) >= 32}
                onClick={() =>
                  updatePolicy({
                    verificationChecks: [
                      ...(policy.verificationChecks ?? []),
                      {
                        id: `check-${(policy.verificationChecks?.length ?? 0) + 1}`,
                        command: "",
                        args: [],
                        cwd: ".",
                        required: true,
                        timeoutMs: 60000,
                        allowTemporaryFiles: false,
                        resultFormat: "exit-code",
                      },
                    ],
                  })
                }
              >
                Add verification check
              </Button>
            </div>
            <Button
              size="sm"
              disabled={!state.policyDirty}
              onClick={() => void controller.savePolicy()}
            >
              Save project policy
            </Button>
          </>
        ) : (
          <p className="text-sm">Loading project policy…</p>
        )}
      </fieldset>
      <Button
        size="sm"
        variant="outline"
        disabled={state.pending || !state.policyDraft}
        onClick={() => void controller.preflight()}
      >
        Check draft provider readiness
      </Button>
      {state.preflight ? (
        <div className="space-y-1 text-sm" role="status">
          <p>
            {state.preflight.ok
              ? "Draft providers are ready. Verification checks have not been executed."
              : "Draft provider configuration needs attention."}
          </p>
          {state.preflight.roles.map((role) => (
            <p key={role.role} className="break-words">
              {role.role}:{" "}
              {role.errorCode ? agentControlPreflightErrorMessage(role.errorCode) : "Ready"}
              {role.candidates
                .filter((candidate) => candidate.errorCode)
                .map(
                  (candidate) =>
                    ` · ${candidate.providerInstanceId}/${candidate.model}: ${agentControlPreflightErrorMessage(candidate.errorCode!)}`,
                )
                .join("")}
            </p>
          ))}
          {!state.preflight.staticPreflight.ok &&
            state.preflight.staticPreflight.errors.map((error) => (
              <p key={JSON.stringify(error)}>
                {error.role}: {agentControlPreflightErrorMessage(error.code)}
              </p>
            ))}
        </div>
      ) : null}
    </details>
  );
}
